#!/bin/bash

# Ceph Device Zapping Script for OpenShift
# This script safely zaps Ceph devices on unmounted storage across OpenShift nodes

set -e

CEPH_IMAGE="quay.io/ceph/ceph:v19"
AUTHFILE="/var/lib/kubelet/config.json"
DRY_RUN=false
FORCE=false

echo "=========================================="
echo "Ceph Device Zapping Script for OpenShift"
echo "=========================================="
echo "This script will zap Ceph devices on unmounted storage across OpenShift nodes"
echo ""

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dry-run          Show what would be done without executing"
    echo "  --force            Skip confirmation prompts"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                 # Interactive mode with confirmations"
    echo "  $0 --dry-run       # Show what would be done"
    echo "  $0 --force         # Skip confirmations"
    echo ""
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if device is mounted
is_device_mounted() {
    local device=$1
    local node=$2
    
    # Check if device is mounted
    local mounted=$(oc debug node/$node -- chroot /host mount | grep -c "/dev/$device" 2>/dev/null || echo "0")
    mounted=$(echo "$mounted" | tr -d '\n\r')
    if [ "$mounted" -gt 0 ]; then
        return 0  # Device is mounted
    fi
    
    # Check if device has partitions that are mounted
    local partitions_mounted=$(oc debug node/$node -- chroot /host mount | grep -c "/dev/$device" 2>/dev/null || echo "0")
    partitions_mounted=$(echo "$partitions_mounted" | tr -d '\n\r')
    if [ "$partitions_mounted" -gt 0 ]; then
        return 0  # Device partitions are mounted
    fi
    
    return 1  # Device is not mounted
}

# Function to check if device is used by LVM
is_device_used_by_lvm() {
    local device=$1
    local node=$2
    
    # Check if device is used by LVM
    local lvm_used=$(oc debug node/$node -- chroot /host pvs /dev/$device 2>/dev/null | wc -l || echo "0")
    lvm_used=$(echo "$lvm_used" | tr -d '\n\r')
    if [ "$lvm_used" -gt 1 ]; then  # More than header line
        return 0  # Device is used by LVM
    fi
    
    return 1  # Device is not used by LVM
}

# Function to check if device is used by mdadm
is_device_used_by_mdadm() {
    local device=$1
    local node=$2
    
    # Check if device is part of an active RAID array by checking /proc/mdstat
    local mdstat_check=$(oc debug node/$node -- chroot /host cat /proc/mdstat 2>/dev/null | grep -c "/dev/$device" || echo "0")
    mdstat_check=$(echo "$mdstat_check" | tr -d '\n\r')
    
    if [ "$mdstat_check" -gt 0 ]; then
        return 0  # Device is part of an active RAID array
    fi
    
    # Also check if device is part of any RAID array using mdadm --detail
    local mdadm_detail=$(oc debug node/$node -- chroot /host mdadm --detail /dev/$device 2>/dev/null | grep -c "State :" || echo "0")
    mdadm_detail=$(echo "$mdadm_detail" | tr -d '\n\r')
    
    if [ "$mdadm_detail" -gt 0 ]; then
        return 0  # Device is a RAID array device
    fi
    
    return 1  # Device is not used by mdadm
}

# Function to check if device is used as swap
is_device_used_as_swap() {
    local device=$1
    local node=$2
    
    # Check if device is used as swap by checking /proc/swaps
    local swap_check=$(oc debug node/$node -- chroot /host cat /proc/swaps 2>/dev/null | grep -c "/dev/$device" || echo "0")
    swap_check=$(echo "$swap_check" | tr -d '\n\r')
    
    if [ "$swap_check" -gt 0 ]; then
        return 0  # Device is used as swap
    fi
    
    # Also check if any partitions of the device are used as swap
    local swap_partition_check=$(oc debug node/$node -- chroot /host cat /proc/swaps 2>/dev/null | grep -c "/dev/$device" || echo "0")
    swap_partition_check=$(echo "$swap_partition_check" | tr -d '\n\r')
    
    if [ "$swap_partition_check" -gt 0 ]; then
        return 0  # Device partition is used as swap
    fi
    
    return 1  # Device is not used as swap
}

# Function to check if device is used by Ceph
is_device_used_by_ceph() {
    local device=$1
    local node=$2
    
    echo "    Checking for Ceph signatures on /dev/$device..."
    
    
    # Method 2: Check using blkid for Ceph filesystem types
    local blkid_check=$(oc debug node/$node -- chroot /host blkid /dev/$device 2>/dev/null | grep -i -E "(ceph|bluestore)" | wc -l || echo "0")
    blkid_check=$(echo "$blkid_check" | tr -d '\n\r')
    if [ "$blkid_check" -gt 0 ]; then
        echo "      ✅ Found Ceph signatures via blkid"
        return 0  # Device has Ceph signatures
    fi
    
    # Method 3: Check using wipefs for Ceph signatures
    local wipefs_check=$(oc debug node/$node -- chroot /host wipefs /dev/$device 2>/dev/null | grep -i -E "(ceph|bluestore)" | wc -l || echo "0")
    wipefs_check=$(echo "$wipefs_check" | tr -d '\n\r')
    if [ "$wipefs_check" -gt 0 ]; then
        echo "      ✅ Found Ceph signatures via wipefs"
        return 0  # Device has Ceph signatures
    fi
    
    # Method 4: Check for Ceph magic bytes at known offsets
    local magic_check=$(oc debug node/$node -- chroot /host dd if=/dev/$device bs=1 count=4 2>/dev/null | od -t x1 | grep -E "(ceph|bluestore)" | wc -l || echo "0")
    magic_check=$(echo "$magic_check" | tr -d '\n\r')
    if [ "$magic_check" -gt 0 ]; then
        echo "      ✅ Found Ceph magic bytes"
        return 0  # Device has Ceph signatures
    fi
    
    # Method 5: Check for LUKS encryption with Ceph labels
    local luks_check=$(oc debug node/$node -- chroot /host cryptsetup luksDump /dev/$device 2>/dev/null | grep -i -E "(ceph|bluestore)" | wc -l || echo "0")
    luks_check=$(echo "$luks_check" | tr -d '\n\r')
    if [ "$luks_check" -gt 0 ]; then
        echo "      ✅ Found Ceph LUKS encryption"
        return 0  # Device has Ceph signatures
    fi
    
    echo "      ℹ️  No Ceph signatures detected"
    return 1  # Device does not have Ceph signatures
}

# Function to check if device is safe to zap
is_device_safe_to_zap() {
    local device=$1
    local node=$2
    
    echo "  Checking device /dev/$device on node $node..."
    
    # Check if device exists
    if ! oc debug node/$node -- chroot /host test -b /dev/$device 2>/dev/null; then
        echo "    ❌ Device /dev/$device does not exist on node $node"
        return 1
    fi
    
    # Check if device is mounted
    if is_device_mounted "$device" "$node"; then
        echo "    ❌ Device /dev/$device is mounted on node $node"
        return 1
    fi
    
    # Check if device is used by LVM
    if is_device_used_by_lvm "$device" "$node"; then
        echo "    ❌ Device /dev/$device is used by LVM on node $node"
        return 1
    fi
    
    # Check if device is used by mdadm
    if is_device_used_by_mdadm "$device" "$node"; then
        echo "    ❌ Device /dev/$device is used by mdadm on node $node"
        return 1
    fi
    
    # Check if device is used as swap
    if is_device_used_as_swap "$device" "$node"; then
        echo "    ❌ Device /dev/$device is used as swap on node $node"
        return 1
    fi
    
    # Check if device has Ceph signatures (this is what we want to zap)
    if ! is_device_used_by_ceph "$device" "$node"; then
        echo "    ⚠️  Device /dev/$device does not have Ceph signatures on node $node"
        echo "    ⚠️  This device may not need zapping"
    fi
    
    echo "    ✅ Device /dev/$device is safe to zap on node $node"
    return 0
}

# Function to get list of storage devices on a node
get_storage_devices() {
    local node=$1
    
    # Get block devices that are likely storage devices
    # Exclude system devices, loop devices, NBD (Network Block Device) devices, and partitions
    local devices=$(oc debug node/$node -- chroot /host lsblk -d -n -o NAME,TYPE,SIZE 2>/dev/null | \
        grep -E 'disk|nvme' | \
        awk '{print $1}' | \
        grep -v -E '^(loop|ram|sr|fd|nbd|nvme)' | \
        sort || true)
    
    echo "$devices"
}

# Function to zap a device on a specific node
zap_device() {
    local device=$1
    local node=$2
    
    echo "  Zapping device /dev/$device on node $node..."
    
    if [ "$DRY_RUN" = true ]; then
        echo "    [DRY RUN] Would execute:"
        echo "    oc debug node/$node -- chroot /host podman run -it --authfile $AUTHFILE --rm --privileged --device /dev/$device --entrypoint ceph-bluestore-tool $CEPH_IMAGE zap-device --dev /dev/$device --yes-i-really-really-mean-it"
        return 0
    fi
    
    # Execute the zap command
    local result=$(oc debug node/$node -- chroot /host podman run -it --authfile $AUTHFILE --rm --privileged --device /dev/$device --entrypoint ceph-bluestore-tool $CEPH_IMAGE zap-device --dev /dev/$device --yes-i-really-really-mean-it 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "    ✅ Successfully zapped device /dev/$device on node $node"
        return 0
    else
        echo "    ❌ Failed to zap device /dev/$device on node $node"
        echo "    Error: $result"
        return 1
    fi
}

# Function to process a single node
process_node() {
    local node=$1
    
    echo "=========================================="
    echo "Processing node: $node"
    echo "=========================================="
    
    # Get list of storage devices on the node
    local devices
    devices=$(get_storage_devices "$node")
    
    if [ -z "$devices" ]; then
        echo "No storage devices found on node $node"
        return 0
    fi
    
    echo "Found storage devices on node $node:"
    for device in $devices; do
        echo "  - /dev/$device"
    done
    echo ""
    
    # Check each device for safety
    local safe_devices=()
    for device in $devices; do
        if is_device_safe_to_zap "$device" "$node"; then
            safe_devices+=("$device")
        fi
        echo ""
    done
    
    if [ ${#safe_devices[@]} -eq 0 ]; then
        echo "No safe devices found on node $node"
        return 0
    fi
    
    echo "Safe devices to zap on node $node:"
    for device in "${safe_devices[@]}"; do
        echo "  - /dev/$device"
    done
    echo ""
    
    # Ask for confirmation if not in force mode
    if [ "$FORCE" = false ] && [ "$DRY_RUN" = false ]; then
        echo "WARNING: This will permanently destroy all data on these devices!"
        read -p "Do you want to zap these devices on node $node? (yes/no): " -r
        if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
            echo "Skipping node $node"
            return 0
        fi
    fi
    
    # Zap each safe device
    local success_count=0
    local total_count=${#safe_devices[@]}
    
    for device in "${safe_devices[@]}"; do
        if zap_device "$device" "$node"; then
            success_count=$((success_count + 1))
        fi
        echo ""
    done
    
    echo "Node $node summary: $success_count/$total_count devices zapped successfully"
}

# Function to get list of OpenShift nodes
get_openshift_nodes() {
    # Get all nodes that are not master/control-plane nodes
    # Exclude nodes with master, control-plane, or similar patterns in their names
    local nodes=$(oc get nodes -o name | sed 's/node\///' | grep -v -E '^(master|control-plane|master-|control-plane-|master\.|control-plane\.)' || true)
    
    # Also filter out nodes that have the control-plane role
    local filtered_nodes=""
    for node in $nodes; do
        # Check if node has control-plane role
        local has_control_plane=$(oc get node $node -o jsonpath='{.metadata.labels}' 2>/dev/null | grep -c "node-role.kubernetes.io/control-plane" || echo "0")
        has_control_plane=$(echo "$has_control_plane" | tr -d '\n\r')
        
        # Check if node has master role (older OpenShift versions)
        local has_master=$(oc get node $node -o jsonpath='{.metadata.labels}' 2>/dev/null | grep -c "node-role.kubernetes.io/master" || echo "0")
        has_master=$(echo "$has_master" | tr -d '\n\r')
        
        # Only include nodes that don't have control-plane or master roles
        if [ "$has_control_plane" -eq 0 ] && [ "$has_master" -eq 0 ]; then
            filtered_nodes="$filtered_nodes $node"
        fi
    done
    
    # Clean up the node list
    filtered_nodes=$(echo "$filtered_nodes" | tr -s ' ' | sed 's/^ *//;s/ *$//')
    
    if [ -z "$filtered_nodes" ]; then
        echo "No worker nodes found (all nodes appear to be master/control-plane nodes)"
        return 1
    fi
    
    echo "$filtered_nodes"
}

# Function to verify prerequisites
verify_prerequisites() {
    echo "Verifying prerequisites..."
    
    # Check if oc command exists
    if ! command_exists oc; then
        echo "❌ Error: oc command not found. Please ensure you're connected to an OpenShift cluster."
        exit 1
    fi
    
    # Check if we can connect to the cluster
    if ! oc get nodes >/dev/null 2>&1; then
        echo "❌ Error: Cannot connect to OpenShift cluster. Please check your connection."
        exit 1
    fi
    
    # Check if we have cluster-admin privileges
    if ! oc auth can-i '*' '*' --all-namespaces >/dev/null 2>&1; then
        echo "❌ Error: Insufficient privileges. Please ensure you have cluster-admin access."
        exit 1
    fi
    
    echo "✅ Prerequisites verified"
}

# Main execution
main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --force)
                FORCE=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    echo "Configuration:"
    echo "  Dry run: $DRY_RUN"
    echo "  Force mode: $FORCE"
    echo "  Ceph image: $CEPH_IMAGE"
    echo "  Auth file: $AUTHFILE"
    echo ""
    
    # Verify prerequisites
    verify_prerequisites
    echo ""
    
    # Get list of OpenShift nodes
    local nodes
    nodes=$(get_openshift_nodes)
    
    if [ -z "$nodes" ]; then
        echo "No OpenShift nodes found"
        exit 1
    fi
    
    echo "Found OpenShift nodes:"
    for node in $nodes; do
        echo "  - $node"
    done
    echo ""
    
    # Ask for confirmation if not in force mode and not dry run
    if [ "$FORCE" = false ] && [ "$DRY_RUN" = false ]; then
        echo "WARNING: This script will zap Ceph devices on unmounted storage across all nodes!"
        echo "This will permanently destroy all data on these devices!"
        echo ""
        read -p "Do you want to continue? (yes/no): " -r
        if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
            echo "Operation cancelled"
            exit 0
        fi
    fi
    
    echo ""
    echo "Starting device zapping process..."
    echo "=========================================="
    
    # Process each node
    local total_success=0
    local total_attempted=0
    
    for node in $nodes; do
        process_node "$node"
        echo ""
    done
    
    echo "=========================================="
    echo "Device zapping process completed!"
    echo "=========================================="
    echo ""
    echo "Summary:"
    echo "  Total nodes processed: $(echo $nodes | wc -w)"
    echo "  Mode: $([ "$DRY_RUN" = true ] && echo "Dry run" || echo "Live execution")"
    echo ""
    echo "Next steps:"
    echo "1. Verify that devices have been properly zapped"
    echo "2. Check for any remaining Ceph signatures: oc debug node/NODE -- chroot /host wipefs -l /dev/DEVICE"
    echo "3. Proceed with ODF installation or reinstallation"
    echo ""
    echo "If you encounter any issues, check the logs above and consider:"
    echo "- Running the device cleanup script: ./cleanup-ceph-devices.sh"
    echo "- Running the OSD metadata cleanup script: ./cleanup-osd-metadata.sh"
}

# Run main function
main "$@"
