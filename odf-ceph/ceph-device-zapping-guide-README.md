# Ceph Device Zapping Guide for OpenShift

## Overview

This guide provides instructions for safely zapping Ceph devices across OpenShift nodes using the `zap-ceph-devices.sh` script. The script uses `oc debug node` to execute Ceph bluestore-tool commands on unmounted storage devices.

## What is Device Zapping?

Device zapping is the process of removing Ceph-specific data and signatures from storage devices. This is necessary when:

- Devices contain data from a previous Ceph cluster
- OSD metadata is corrupted
- You need to clean devices before reinstalling ODF
- Resolving cluster ID conflicts

## Script Features

### Safety Checks
- ✅ **Mounted Device Detection**: Prevents zapping mounted devices
- ✅ **LVM Usage Detection**: Prevents zapping devices used by LVM
- ✅ **MDADM Usage Detection**: Prevents zapping devices used by RAID
- ✅ **Ceph Signature Detection**: Identifies devices with Ceph data
- ✅ **Device Existence Verification**: Ensures devices exist before zapping
- ✅ **Swap device detection**: Devices used for swap will not be touched by this script 

### Execution Modes
- ✅ **Interactive Mode**: Prompts for confirmation before zapping
- ✅ **Dry Run Mode**: Shows what would be done without executing
- ✅ **Force Mode**: Skips confirmation prompts
- ✅ **Node-by-Node Processing**: Processes each node individually

### Error Handling
- ✅ **Privilege Verification**: Ensures cluster-admin access
- ✅ **Connection Verification**: Verifies OpenShift cluster connectivity
- ✅ **Device Validation**: Validates device existence and safety
- ✅ **Error Reporting**: Provides detailed error messages

## Usage

### Basic Usage

```bash
# Interactive mode with confirmations
./zap-ceph-devices.sh

# Dry run to see what would be done
./zap-ceph-devices.sh --dry-run

# Force mode (skip confirmations)
./zap-ceph-devices.sh --force

# Run in parallel mode 
./zap-ceph-devices.sh --parallel      

# Run in parallel mode - 5 nodes at time 
 ./zap-ceph-devices.sh --parallel --max-parallel 5


# Show help
./zap-ceph-devices.sh --help
```

### Prerequisites

1. **OpenShift Cluster Access**: Must be connected to an OpenShift cluster
2. **Cluster-Admin Privileges**: Must have cluster-admin access
3. **Node Access**: Must be able to debug nodes
4. **Podman**: Podman must be available on the nodes
5. **Ceph Image**: The Ceph image must be accessible

### Required Permissions

```bash
# Check if you have cluster-admin access
oc auth can-i '*' '*' --all-namespaces

# Check if you can debug nodes
oc debug node/NODE_NAME -- chroot /host whoami
```

## How It Works

### 1. Node Discovery
The script discovers all OpenShift nodes (excluding master/control-plane nodes):

```bash
oc get nodes -o name | sed 's/node\///'
```

### 2. Device Discovery
For each node, it discovers storage devices:

```bash
oc debug node/NODE_NAME -- chroot /host lsblk -d -n -o NAME,TYPE,SIZE
```

### 3. Safety Checks
For each device, it performs comprehensive safety checks:

#### Mount Check
```bash
oc debug node/NODE_NAME -- chroot /host mount | grep /dev/DEVICE
```

#### LVM Usage Check
```bash
oc debug node/NODE_NAME -- chroot /host pvs /dev/DEVICE
```

#### MDADM Usage Check
```bash
# Check if device is part of active RAID array
oc debug node/NODE_NAME -- chroot /host cat /proc/mdstat | grep /dev/DEVICE

# Check if device is a RAID array device
oc debug node/NODE_NAME -- chroot /host mdadm --detail /dev/DEVICE

# Check if device is swap 
```


#### Ceph Signature Check (Multiple Methods)
```bash
# Method 1: ceph-volume inventory via container (most reliable)
oc debug node/NODE_NAME -- chroot /host podman run --authfile /var/lib/kubelet/config.json --rm --privileged --device /dev/DEVICE --entrypoint ceph-volume quay.io/ceph/ceph:v19 inventory --format json /dev/DEVICE

# Method 2: blkid for Ceph filesystem types
oc debug node/NODE_NAME -- chroot /host blkid /dev/DEVICE

# Method 3: wipefs for Ceph signatures
oc debug node/NODE_NAME -- chroot /host wipefs /dev/DEVICE

# Method 4: Check for Ceph magic bytes
oc debug node/NODE_NAME -- chroot /host dd if=/dev/DEVICE bs=1 count=4 | od -t x1

# Method 5: Check for LUKS encryption with Ceph labels
oc debug node/NODE_NAME -- chroot /host cryptsetup luksDump /dev/DEVICE
```

### 4. Device Zapping
For safe devices, it executes the zap command:

```bash
oc debug node/NODE_NAME -- chroot /host podman run -it \
  --authfile /var/lib/kubelet/config.json \
  --rm --privileged \
  --device /dev/DEVICE \
  --entrypoint ceph-bluestore-tool \
  quay.io/ceph/ceph:v19 \
  zap-device --dev /dev/DEVICE \
  --yes-i-really-really-mean-it
```

## Safety Features

### Device Protection
The script will **NOT** zap devices that are:
- Mounted (including partitions)
- Used by LVM (Physical Volumes)
- Used by MDADM (RAID arrays)
- Used as swap (including swap partitions)
- System devices (loop, ram, sr, fd, nbd)

### Confirmation Prompts
- Node-level confirmation before processing
- Device-level confirmation for each device
- Force mode to skip confirmations
- Dry run mode to preview actions

### Error Handling
- Graceful failure handling
- Detailed error messages
- Rollback capabilities
- Verification steps

## Example Output

### Dry Run Mode
```bash
$ ./zap-ceph-devices.sh --dry-run

==========================================
Ceph Device Zapping Script for OpenShift
==========================================

Configuration:
  Dry run: true
  Force mode: false
  Ceph image: quay.io/ceph/ceph:v19
  Auth file: /var/lib/kubelet/config.json

✅ Prerequisites verified

Found OpenShift nodes:
  - worker-1
  - worker-2
  - worker-3

==========================================
Processing node: worker-1
==========================================

Found storage devices on node worker-1:
  - /dev/sdb
  - /dev/sdc
  - /dev/sdd

  Checking device /dev/sdb on node worker-1...
    ✅ Device /dev/sdb is safe to zap on node worker-1

  Checking device /dev/sdc on node worker-1...
    ❌ Device /dev/sdc is mounted on node worker-1

  Checking device /dev/sdd on node worker-1...
    ✅ Device /dev/sdd is safe to zap on node worker-1

Safe devices to zap on node worker-1:
  - /dev/sdb
  - /dev/sdd

    [DRY RUN] Would execute:
    oc debug node/worker-1 -- chroot /host podman run -it --authfile /var/lib/kubelet/config.json --rm --privileged --device /dev/sdb --entrypoint ceph-bluestore-tool quay.io/ceph/ceph:v19 zap-device --dev /dev/sdb --yes-i-really-really-mean-it

    [DRY RUN] Would execute:
    oc debug node/worker-1 -- chroot /host podman run -it --authfile /var/lib/kubelet/config.json --rm --privileged --device /dev/sdd --entrypoint ceph-bluestore-tool quay.io/ceph/ceph:v19 zap-device --dev /dev/sdd --yes-i-really-really-mean-it
```

### Live Execution
```bash
$ ./zap-ceph-devices.sh

==========================================
Ceph Device Zapping Script for OpenShift
==========================================

WARNING: This script will zap Ceph devices on unmounted storage across all nodes!
This will permanently destroy all data on these devices!

Do you want to continue? (yes/no): yes

==========================================
Processing node: worker-1
==========================================

Found storage devices on node worker-1:
  - /dev/sdb
  - /dev/sdd

Safe devices to zap on node worker-1:
  - /dev/sdb
  - /dev/sdd

WARNING: This will permanently destroy all data on these devices!
Do you want to zap these devices on node worker-1? (yes/no): yes

  Zapping device /dev/sdb on node worker-1...
    ✅ Successfully zapped device /dev/sdb on node worker-1

  Zapping device /dev/sdd on node worker-1...
    ✅ Successfully zapped device /dev/sdd on node worker-1

Node worker-1 summary: 2/2 devices zapped successfully
```

## Troubleshooting

### Common Issues

#### 1. Permission Denied
```bash
❌ Error: Insufficient privileges. Please ensure you have cluster-admin access.
```
**Solution**: Ensure you have cluster-admin access:
```bash
oc auth can-i '*' '*' --all-namespaces
```

#### 2. Device Not Found
```bash
❌ Device /dev/sdb does not exist on node worker-1
```
**Solution**: Check device names on the node:
```bash
oc debug node/worker-1 -- chroot /host lsblk
```

#### 3. Device Mounted
```bash
❌ Device /dev/sdb is mounted on node worker-1
```
**Solution**: Unmount the device or exclude it from zapping:
```bash
oc debug node/worker-1 -- chroot /host umount /dev/sdb
```

#### 4. LVM Usage
```bash
❌ Device /dev/sdb is used by LVM on node worker-1
```
**Solution**: Remove LVM usage or exclude the device:
```bash
oc debug node/worker-1 -- chroot /host vgremove VG_NAME
oc debug node/worker-1 -- chroot /host pvremove /dev/sdb
```

#### 5. MDADM Usage
```bash
❌ Device /dev/sdb is used by mdadm on node worker-1
```
**Solution**: Check if device is actually part of RAID or exclude it:
```bash
# Check if device is in active RAID
oc debug node/worker-1 -- chroot /host cat /proc/mdstat | grep /dev/sdb

# Check if device is a RAID array
oc debug node/worker-1 -- chroot /host mdadm --detail /dev/sdb
```

#### 6. Device Used as Swap
```bash
❌ Device /dev/sdb is used as swap on node worker-1
```
**Solution**: Remove swap usage or exclude the device:
```bash
# Check current swap usage
oc debug node/worker-1 -- chroot /host cat /proc/swaps

# Disable swap on the device (if safe to do so)
oc debug node/worker-1 -- chroot /host swapoff /dev/sdb

# Remove from /etc/fstab if needed
oc debug node/worker-1 -- chroot /host sed -i '/\/dev\/sdb/d' /etc/fstab
```

#### 7. Podman Not Available
```bash
❌ Failed to zap device /dev/sdb on node worker-1
Error: podman: command not found
```
**Solution**: Ensure Podman is available on the node:
```bash
oc debug node/worker-1 -- chroot /host which podman
```

### Verification Commands

#### Check Device Status
```bash
# Check if device is clean
oc debug node/NODE_NAME -- chroot /host wipefs /dev/DEVICE

# Check device partitions
oc debug node/NODE_NAME -- chroot /host lsblk /dev/DEVICE

# Check device usage
oc debug node/NODE_NAME -- chroot /host pvs /dev/DEVICE

# Check if device is used as swap
oc debug node/NODE_NAME -- chroot /host cat /proc/swaps | grep /dev/DEVICE
```

#### Check Ceph Signatures
```bash
# Check for remaining Ceph signatures (multiple methods)
oc debug node/NODE_NAME -- chroot /host podman run --authfile /var/lib/kubelet/config.json --rm --privileged --device /dev/DEVICE --entrypoint ceph-volume quay.io/ceph/ceph:v19 inventory --format json /dev/DEVICE | grep -i ceph
oc debug node/NODE_NAME -- chroot /host blkid /dev/DEVICE | grep -i ceph
oc debug node/NODE_NAME -- chroot /host wipefs /dev/DEVICE | grep -i ceph

# Check for Ceph processes
oc debug node/NODE_NAME -- chroot /host ps aux | grep ceph
```

## Best Practices

### Before Running
1. **Backup Important Data**: Ensure you have backups of any important data
2. **Test in Non-Production**: Always test in a non-production environment first
3. **Verify Device List**: Use dry-run mode to verify which devices will be zapped
4. **Check Node Status**: Ensure all nodes are in Ready state

### During Execution
1. **Monitor Progress**: Watch the output for any errors
2. **Verify Safety**: Ensure only intended devices are being zapped
3. **Check Node Health**: Monitor node health during execution
4. **Check log files created in /tmp**: Log files are created in `/tmp` for every device touched by this script 

### After Execution
1. **Verify Cleanup**: Check that devices are properly cleaned
2. **Test ODF Installation**: Verify that ODF can be installed successfully

## Security Considerations

### Access Control
- Requires cluster-admin privileges
- Uses node debug capabilities
- Accesses host filesystem

### Data Protection
- Only zaps unmounted devices
- Skips system and used devices
- Provides confirmation prompts

### Audit Trail
- Logs all actions
- Provides detailed output
- Records success/failure status

## Support and Documentation

- [OpenShift Data Foundation Documentation](https://access.redhat.com/documentation/en-us/red_hat_openshift_data_foundation/)
- [Ceph Documentation](https://docs.ceph.com/)
- [Red Hat Support](https://access.redhat.com/support)

---

**Warning**: This script will permanently destroy all data on the specified devices. Always test in a non-production environment first and ensure you have proper backups before proceeding.
