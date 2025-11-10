#!/usr/bin/env python3
"""
FIO Remote Testing Script
This script executes FIO performance tests on remote machines/VMs via SSH
Supports YAML configuration and multiple machines/VMs testing
"""

import argparse
import base64
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging early (before dependency checks)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import required dependencies
try:
    import yaml
except ImportError:
    logger.error("PyYAML is required but not installed.")
    logger.error("Please install dependencies first:")
    logger.error("  pip install -r requirements.txt")
    logger.error("Or install PyYAML directly:")
    logger.error("  pip install PyYAML>=5.4.1")
    sys.exit(1)


class FioTestConfig:
    """Configuration class for FIO tests"""
    
    def __init__(self):
        # These values must be set in YAML config (no defaults to avoid masking)
        self.config_file = "fio-config.yaml"
        self.dry_run = False
        self.verbose = False
        self.use_virtctl = None  # None = auto-detect, True = force virtctl, False = force SSH
        self.skip_confirmation = False
        self.prepare_machine = False
        self.retry_interval = None
        self.max_retries = None
        self.skip_connectivity_test = False
        self.task_monitor_interval = None
        self.debug_config = False
        self.namespace = None
        self.vm_hosts = []
        # These values must be set in YAML config (no defaults to avoid masking)
        self.mount_point = None
        self.filesystem = None
        self.test_size = None
        self.test_runtime = None
        self.block_sizes = []
        self.io_patterns = []
        self.numjobs = 1
        self.iodepth = 1
        self.direct_io = "1"
        self.rate_iops = None
        self.output_dir = None
        self.output_format = None
        self.description = ""
        self.migrate_workloads = []
        self.migrate_interval = 0
        self.storage_devices = {}  # host -> device mapping


class CommandExecutor:
    """Handles command execution via SSH or virtctl"""
    
    def __init__(self, config: FioTestConfig):
        self.config = config
    
    def is_vm_host(self, host: str) -> bool:
        """Check if host is a VM"""
        if self.config.use_virtctl is False:
            return False
        if self.config.use_virtctl is True:
            return True
        
        # Auto-detection: check if VM exists in namespace
        if not self.config.namespace or self.config.namespace == "N/A":
            return False
        
        try:
            result = subprocess.run(
                ["oc", "get", "vm", host, "-n", self.config.namespace],
                capture_output=True,
                timeout=10
            )
            if result.returncode == 0:
                return True
            
            # Check for VMI
            result = subprocess.run(
                ["oc", "get", "vmi", host, "-n", self.config.namespace],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
    
    def get_ssh_command(self, host: str, command: str) -> List[str]:
        """Get SSH command for host"""
        if self.is_vm_host(host):
            if not self.config.namespace or self.config.namespace == "N/A":
                raise ValueError(f"NAMESPACE is not set but host '{host}' is detected as a VM")
            return [
                "virtctl", "-n", self.config.namespace, "ssh",
                "--local-ssh-opts=-o StrictHostKeyChecking=no",
                f"root@vmi/{host}", "-c", command
            ]
        else:
            return [
                "ssh", "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                f"root@{host}", command
            ]
    
    def get_scp_command(self, source: str, destination: str) -> List[str]:
        """Get SCP command for copying files"""
        # Extract hostname from source
        host_match = re.search(r'root@vmi/([^:]+):', source) or re.search(r'root@([^:]+):', source)
        if not host_match:
            raise ValueError(f"Cannot extract hostname from source: {source}")
        
        host = host_match.group(1)
        
        if self.is_vm_host(host):
            if not self.config.namespace or self.config.namespace == "N/A":
                raise ValueError(f"NAMESPACE is not set but host '{host}' is detected as a VM")
            return [
                "virtctl", "-n", self.config.namespace, "scp",
                "--local-ssh-opts=-o StrictHostKeyChecking=no",
                source, destination
            ]
        else:
            # Convert virtctl format to SSH format
            ssh_source = source.replace("root@vmi/", "root@")
            return [
                "scp", "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                ssh_source, destination
            ]
    
    def execute_command(self, host: str, command: str, description: str = "command",
                       max_retries: Optional[int] = None,
                       retry_interval: Optional[int] = None,
                       timeout: Optional[int] = None) -> Tuple[bool, str]:
        """Execute command on remote host with retry logic"""
        # Use provided values or fall back to config (which must be set)
        max_retries = max_retries if max_retries is not None else self.config.max_retries
        retry_interval = retry_interval if retry_interval is not None else self.config.retry_interval
        # Default timeout is 300s, but can be overridden for quick commands
        cmd_timeout = timeout if timeout is not None else 300
        
        if max_retries is None or retry_interval is None:
            logger.error("CRITICAL: retry_interval and max_retries must be set in configuration")
            sys.exit(1)
        
        if self.config.dry_run:
            logger.info(f"DRY-RUN: Would execute on {host}: {command}")
            return True, ""
        
        ssh_cmd = self.get_ssh_command(host, command)
        
        for attempt in range(1, max_retries + 1):
            try:
                result = subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    text=True,
                    timeout=cmd_timeout
                )
                
                if result.returncode == 0:
                    if self.config.verbose and result.stdout:
                        logger.info(f"Command output from {host}: {result.stdout}")
                    return True, result.stdout
                
                if attempt < max_retries:
                    logger.warning(f"Command failed on {host} (attempt {attempt}/{max_retries}): {description}")
                    if self.config.verbose:
                        logger.warning(f"Exit code: {result.returncode}")
                        logger.warning(f"Error output: {result.stderr}")
                    logger.warning(f"Retrying in {retry_interval}s...")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Failed to execute '{description}' on {host} after {max_retries} attempts")
                    return False, result.stderr
                    
            except subprocess.TimeoutExpired:
                # For quick checks (short timeout), use warning instead of error
                if cmd_timeout <= 30:
                    logger.warning(f"Command timeout on {host}: {description} (timeout: {cmd_timeout}s)")
                else:
                    logger.error(f"Command timeout on {host}: {description} (timeout: {cmd_timeout}s)")
                return False, "Command timeout"
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"Command error on {host} (attempt {attempt}/{max_retries}): {str(e)}")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Command exception on {host}: {str(e)}")
                    return False, str(e)
        
        return False, "Max retries exceeded"
    
    def execute_background(self, host: str, command: str, description: str = "background command",
                          migration_state: Optional[Dict[str, bool]] = None) -> threading.Thread:
        """Execute command in background thread"""
        def run_command():
            # Check if long-running command (FIO with runtime)
            use_nohup = False
            runtime_value = None
            
            if "--runtime" in command:
                runtime_match = re.search(r'--runtime[=\s]+(\d+)', command)
                if runtime_match:
                    runtime_value = int(runtime_match.group(1))
                    if runtime_value > 30:  # Default threshold
                        use_nohup = True
            
            if "fio" in command and "--runtime" not in command:
                use_nohup = True
            
            if use_nohup:
                logger.info(f"Detected long-running command - will use nohup to allow SSH disconnection")
                # Create temporary script on remote VM
                script_file = f"/tmp/fio_run_{int(time.time())}_{os.getpid()}.sh"
                log_file = f"/tmp/fio_background_{int(time.time())}_{os.getpid()}.log"
                
                # Encode command using base64
                encoded_cmd = base64.b64encode(command.encode()).decode()
                
                # Create script that decodes and runs command
                script_cmd = (
                    f"echo '{encoded_cmd}' | base64 -d > {script_file} && "
                    f"chmod +x {script_file} && "
                    f"nohup bash {script_file} > {log_file} 2>&1 & "
                    f"sleep 2 && "
                    f"ps aux | grep -E 'fio.*testfile' | grep -v grep | head -1 | awk '{{print $2}}' || echo '0'"
                )
                
                # Use shorter timeout (60s) for nohup setup - it should complete quickly
                # If it times out, the process might still be running, so we'll verify separately
                success, output = self.execute_command(host, script_cmd, description, timeout=60)
                if success:
                    pid = re.search(r'\d+', output)
                    if pid and pid.group() != "0":
                        logger.info(f"Background FIO process started on {host} with PID: {pid.group()}")
                        return
                    else:
                        # Script executed but PID not found - verify process is actually running
                        logger.warning(f"PID not found in output for {host}, verifying FIO process is running...")
                        time.sleep(3)  # Give it a moment to start
                        if self.check_task_running(host, "fio.*testfile"):
                            logger.info(f"Background FIO process confirmed running on {host}")
                            return
                        else:
                            logger.warning(f"FIO process may not have started on {host} - will be checked later")
                            return
                else:
                    # Command failed or timed out, but nohup might have still started the process
                    # Verify if process is actually running before reporting failure
                    logger.warning(f"SSH verification timed out on {host}, checking if process started...")
                    time.sleep(3)  # Give it a moment to start
                    if self.check_task_running(host, "fio.*testfile"):
                        logger.info(f"Process confirmed running on {host} despite SSH timeout - nohup started successfully")
                        return
                    # If we get here, the process really didn't start
                    logger.error(f"Failed to start background FIO process on {host} - verification confirms process is not running")
                    return
            else:
                self.execute_command(host, command, description)
        
        thread = threading.Thread(target=run_command, daemon=True)
        thread.start()
        return thread
    
    def check_task_running(self, host: str, task_pattern: str = "fio.*testfile") -> bool:
        """Check if a task is running on a host"""
        cmd = f"ps aux | grep -E '{task_pattern}' | grep -v grep | wc -l"
        # Use a short timeout (30s) for quick process checks
        success, output = self.execute_command(host, cmd, "Checking task status", max_retries=1, retry_interval=1, timeout=30)
        if success:
            try:
                count = int(output.strip())
                return count > 0
            except ValueError:
                return False
        # If check fails or times out, assume task is not running (fail-safe)
        return False


class ConfigLoader:
    """Loads and validates configuration from YAML file"""
    
    def __init__(self, config: FioTestConfig):
        self.config = config
    
    def load_config(self) -> None:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config.config_file):
            logger.error(f"Configuration file '{self.config.config_file}' not found")
            sys.exit(1)
        
        with open(self.config.config_file, 'r') as f:
            yaml_data = yaml.safe_load(f)
        
        # Load namespace
        if self.config.use_virtctl is not False:
            self.config.namespace = yaml_data.get('vm', {}).get('namespace', 'default')
            if self.config.namespace == "null":
                self.config.namespace = "default"
        else:
            self.config.namespace = "N/A"
        
        # Load VM hosts
        self.config.vm_hosts = self._get_vm_hosts(yaml_data)
        
        # Load storage configuration (required)
        storage = yaml_data.get('storage', {})
        if not storage:
            logger.error("CRITICAL: 'storage' section is required in configuration file")
            sys.exit(1)
        
        if 'mount_point' not in storage or not storage.get('mount_point') or storage.get('mount_point') == "null":
            logger.error("CRITICAL: 'storage.mount_point' is required in configuration file")
            sys.exit(1)
        self.config.mount_point = storage['mount_point']
        
        if 'filesystem' not in storage or not storage.get('filesystem') or storage.get('filesystem') == "null":
            logger.error("CRITICAL: 'storage.filesystem' is required in configuration file")
            sys.exit(1)
        self.config.filesystem = storage['filesystem']
        
        # Load device mappings
        devices = storage.get('devices', {})
        for host in self.config.vm_hosts:
            device = devices.get(host)
            if not device:
                # Try pattern matching
                device = self._get_device_from_pattern(host, devices)
            if device:
                self.config.storage_devices[host] = device
            else:
                logger.error(f"CRITICAL: No storage device specified for host '{host}'")
                sys.exit(1)
        
        # Load FIO configuration
        fio = yaml_data.get('fio', {})
        self.config.test_size = fio.get('test_size')
        # Ensure test_runtime is an integer
        runtime = fio.get('runtime')
        if isinstance(runtime, str):
            self.config.test_runtime = int(runtime)
        else:
            self.config.test_runtime = int(runtime) if runtime else None
        self.config.block_sizes = fio.get('block_sizes', '').split()
        self.config.io_patterns = fio.get('io_patterns', '').split()
        self.config.numjobs = int(fio.get('numjobs', 1))
        self.config.iodepth = int(fio.get('iodepth', 1))
        self.config.direct_io = str(fio.get('direct_io', 1))
        self.config.rate_iops = fio.get('rate_iops')
        if self.config.rate_iops == "null" or not self.config.rate_iops:
            self.config.rate_iops = None
        else:
            # Ensure rate_iops is an integer if it's set
            if isinstance(self.config.rate_iops, str):
                self.config.rate_iops = int(self.config.rate_iops)
        
        # Load output configuration (required)
        output = yaml_data.get('output', {})
        if not output:
            logger.error("CRITICAL: 'output' section is required in configuration file")
            sys.exit(1)
        
        if 'directory' not in output or not output.get('directory') or output.get('directory') == "null":
            logger.error("CRITICAL: 'output.directory' is required in configuration file")
            sys.exit(1)
        self.config.output_dir = output['directory']
        
        if 'format' not in output or not output.get('format') or output.get('format') == "null":
            logger.error("CRITICAL: 'output.format' is required in configuration file")
            sys.exit(1)
        self.config.output_format = output['format']
        
        self.config.description = yaml_data.get('description', '')
        if self.config.description == "null" or not self.config.description:
            self.config.description = ""
        
        # Load retry configuration (required)
        retry = yaml_data.get('retry', {})
        if not retry:
            logger.error("CRITICAL: 'retry' section is required in configuration file")
            sys.exit(1)
        
        if 'interval' not in retry or retry.get('interval') is None:
            logger.error("CRITICAL: 'retry.interval' is required in configuration file")
            sys.exit(1)
        self.config.retry_interval = int(retry['interval'])
        
        if 'max_retries' not in retry or retry.get('max_retries') is None:
            logger.error("CRITICAL: 'retry.max_retries' is required in configuration file")
            sys.exit(1)
        self.config.max_retries = int(retry['max_retries'])
        
        if retry.get('skip_connectivity_test'):
            self.config.skip_connectivity_test = retry['skip_connectivity_test']
        
        # Load monitoring configuration (required)
        monitoring = yaml_data.get('monitoring', {})
        if not monitoring:
            logger.error("CRITICAL: 'monitoring' section is required in configuration file")
            sys.exit(1)
        
        if 'task_monitor_interval' not in monitoring or monitoring.get('task_monitor_interval') is None:
            logger.error("CRITICAL: 'monitoring.task_monitor_interval' is required in configuration file")
            sys.exit(1)
        self.config.task_monitor_interval = int(monitoring['task_monitor_interval'])
        
        # Load migration configuration
        migrate = yaml_data.get('migrate')
        if migrate is None or migrate == "null":
            # No migration configuration or explicitly null
            self.config.migrate_workloads = []
            self.config.migrate_interval = 0
        else:
            # migrate is a dictionary
            migrate_workloads = migrate.get('workloads', '')
            if migrate_workloads and migrate_workloads != "null":
                self.config.migrate_workloads = migrate_workloads.split()
            else:
                self.config.migrate_workloads = []
            
            migrate_interval = migrate.get('interval', 0)
            if migrate_interval == "null" or not migrate_interval:
                self.config.migrate_interval = 0
            else:
                self.config.migrate_interval = int(migrate_interval)
    
    def _get_vm_hosts(self, yaml_data: Dict) -> List[str]:
        """Get VM hosts from various methods"""
        vm_config = yaml_data.get('vm', {})
        
        # Method 1: Host pattern
        host_pattern = vm_config.get('host_pattern')
        if host_pattern:
            logger.info(f"Using host pattern: {host_pattern}")
            # Expand pattern like vm{1..200} or vme-{1..10}
            if '{' in host_pattern and '..' in host_pattern:
                # Match pattern with optional dashes/underscores: prefix{start..end}
                # Examples: vm{1..5}, vme-{1..10}, host_{1..100}
                match = re.search(r'([\w-]+)\{(\d+)\.\.(\d+)\}', host_pattern)
                if match:
                    prefix = match.group(1)
                    start = int(match.group(2))
                    end = int(match.group(3))
                    expanded = [f"{prefix}{i}" for i in range(start, end + 1)]
                    logger.info(f"Expanded pattern to {len(expanded)} hosts: {expanded[:5]}{'...' if len(expanded) > 5 else ''}")
                    return expanded
                else:
                    logger.warning(f"Could not parse host pattern '{host_pattern}' - using as-is")
            return [host_pattern]
        
        # Method 2: Host labels
        host_labels = vm_config.get('host_labels')
        if host_labels:
            if self.config.use_virtctl is False:
                logger.error("Label-based host selection is not supported in SSH-only mode")
                sys.exit(1)
            logger.info(f"Using label selector: {host_labels}")
            if not self.config.dry_run:
                try:
                    result = subprocess.run(
                        ["oc", "get", "vms", "-n", self.config.namespace,
                         "-l", host_labels, "-o", "jsonpath={range .items[*]}{.metadata.name}{' '}{end}"],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        hosts = result.stdout.strip().split()
                        logger.info(f"Found {len(hosts)} VMs matching labels: {host_labels}")
                        return hosts
                except Exception as e:
                    logger.warning(f"Failed to query VMs by labels: {e}")
        
        # Method 3: Host file
        host_file = vm_config.get('host_file')
        if host_file:
            logger.info(f"Using host file: {host_file}")
            if os.path.exists(host_file):
                hosts = []
                with open(host_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            # Handle patterns in file
                            if '{' in line and '..' in line:
                                match = re.search(r'([\w-]+)\{(\d+)\.\.(\d+)\}', line)
                                if match:
                                    prefix = match.group(1)
                                    start = int(match.group(2))
                                    end = int(match.group(3))
                                    hosts.extend([f"{prefix}{i}" for i in range(start, end + 1)])
                            else:
                                hosts.append(line)
                if hosts:
                    logger.info(f"Loaded {len(hosts)} hosts from file: {host_file}")
                    return hosts
        
        # Method 4: Simple host list
        hosts = vm_config.get('hosts')
        if hosts:
            logger.info(f"Using simple host list: {hosts}")
            return hosts.split() if isinstance(hosts, str) else hosts
        
        logger.error("No hosts specified in configuration")
        sys.exit(1)
    
    def _get_device_from_pattern(self, host: str, devices: Dict) -> Optional[str]:
        """Get device from pattern matching"""
        for pattern, device in devices.items():
            if '{' in pattern and '..' in pattern:
                match = re.search(r'([\w-]+)\{(\d+)\.\.(\d+)\}', pattern)
                if match:
                    prefix = match.group(1)
                    start = int(match.group(2))
                    end = int(match.group(3))
                    for i in range(start, end + 1):
                        if f"{prefix}{i}" == host:
                            return device
        return None


def check_dependencies(config: FioTestConfig) -> None:
    """Check if required tools are installed"""
    missing_tools = []
    
    if not config.dry_run:
        if config.use_virtctl is True:
            # Force virtctl mode
            if not shutil.which("virtctl"):
                missing_tools.append("virtctl")
            if not shutil.which("oc"):
                missing_tools.append("oc")
        elif config.use_virtctl is False:
            # Force SSH mode
            if not shutil.which("ssh"):
                missing_tools.append("ssh")
        else:
            # Auto-detection mode
            if not shutil.which("virtctl"):
                missing_tools.append("virtctl")
            if not shutil.which("oc"):
                missing_tools.append("oc")
            if not shutil.which("ssh"):
                missing_tools.append("ssh")
    
    if missing_tools:
        logger.error("The following required tools are missing:")
        for tool in missing_tools:
            logger.error(f"  - {tool}")
        sys.exit(1)


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="FIO Remote Testing Script (Python version)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('-c', '--config', default='fio-config.yaml',
                       help='Path to YAML configuration file (default: fio-config.yaml)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('--dry-run', action='store_true',
                       help='Validate configuration and show what would be done without executing')
    parser.add_argument('--ssh-only', action='store_true',
                       help='Force SSH for all hosts')
    parser.add_argument('--virtctl-only', action='store_true',
                       help='Force virtctl for all hosts')
    parser.add_argument('--yes-i-mean-it', action='store_true',
                       help='Skip confirmation prompt for device formatting')
    parser.add_argument('--prepare-machine', action='store_true',
                       help='Only install FIO dependencies on machines, skip all testing')
    parser.add_argument('--interval', type=int,
                       help='Override retry interval in seconds (from config file)')
    parser.add_argument('--max-retries', type=int,
                       help='Override maximum number of retry attempts (from config file)')
    parser.add_argument('--skip-connectivity-test', action='store_true',
                       help='Skip connectivity test and proceed directly to command execution')
    parser.add_argument('--monitor-interval', type=int,
                       help='Override task monitor interval in seconds (from config file)')
    parser.add_argument('--debug', action='store_true',
                       help='Show detailed configuration parsing debug information')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting FIO remote testing script (Python version)")
    
    # Initialize configuration
    config = FioTestConfig()
    config.config_file = args.config
    config.dry_run = args.dry_run
    config.verbose = args.verbose
    config.use_virtctl = None if not (args.ssh_only or args.virtctl_only) else (not args.ssh_only)
    config.skip_confirmation = args.yes_i_mean_it
    config.prepare_machine = args.prepare_machine
    # Override config values with command-line arguments if provided
    if args.interval is not None:
        config.retry_interval = args.interval
    if args.max_retries is not None:
        config.max_retries = args.max_retries
    config.skip_connectivity_test = args.skip_connectivity_test
    if args.monitor_interval is not None:
        config.task_monitor_interval = args.monitor_interval
    config.debug_config = args.debug
    
    # Load configuration
    config_loader = ConfigLoader(config)
    config_loader.load_config()
    
    # Set up log file with description in filename
    log_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    sanitized_desc = re.sub(r'[^a-z0-9]', '_', config.description.lower()) if config.description else ""
    sanitized_desc = re.sub(r'_+', '_', sanitized_desc).strip('_')
    
    if sanitized_desc:
        log_file = f"fio-test-{sanitized_desc}-{log_timestamp}.txt"
    else:
        log_file = f"fio-test-{log_timestamp}.txt"
    
    # Add file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s',
                                                datefmt='%Y-%m-%d %H:%M:%S'))
    logging.getLogger().addHandler(file_handler)
    
    logger.info(f"Logging all output to: {log_file}")
    
    # Add description to log file header
    if config.description:
        logger.info("=" * 80)
        logger.info(f"TEST DESCRIPTION: {config.description}")
        logger.info("=" * 80)
    
    # Check dependencies
    check_dependencies(config)
    
    # Display configuration
    logger.info(f"Configuration loaded from: {config.config_file}")
    logger.info(f"VMs: {' '.join(config.vm_hosts)}")
    if config.use_virtctl is not False:
        logger.info(f"Namespace: {config.namespace}")
    else:
        logger.info("Namespace: N/A (SSH-only mode)")
    
    logger.info(f"Storage device configuration:")
    for host in config.vm_hosts:
        device = config.storage_devices.get(host, "N/A")
        logger.info(f"  {host}: /dev/{device}")
    
    logger.info(f"Mount point: {config.mount_point}")
    logger.info(f"Filesystem: {config.filesystem}")
    logger.info(f"Test size: {config.test_size}")
    logger.info(f"Runtime: {config.test_runtime}s")
    logger.info(f"Block sizes: {' '.join(config.block_sizes)}")
    logger.info(f"I/O patterns: {' '.join(config.io_patterns)}")
    
    if config.migrate_workloads:
        if config.migrate_interval > 0:
            logger.info(f"VM Migration: ENABLED for patterns: {' '.join(config.migrate_workloads)} "
                       f"(sequential with {config.migrate_interval}s interval)")
        else:
            logger.info(f"VM Migration: ENABLED for patterns: {' '.join(config.migrate_workloads)} (parallel)")
    else:
        logger.info("VM Migration: DISABLED")
    
    if config.dry_run:
        logger.info("DRY RUN MODE: Configuration validated successfully")
        logger.info("Would execute the following steps:")
        logger.info("  1. Install FIO and dependencies on VMs")
        logger.info("  2. Prepare storage (format and mount devices)")
        logger.info("  3. Write initial test dataset")
        logger.info("  4. Run FIO performance tests")
        logger.info("  5. Collect test results")
        logger.info("  6. Clean up test environment")
        return 0
    
    # Handle prepare-machine mode
    if config.prepare_machine:
        logger.info("PREPARE MACHINE MODE: Installing FIO dependencies only")
        logger.info(f"Using retry configuration: interval={config.retry_interval}s, max_retries={config.max_retries}")
        if not config.skip_connectivity_test:
            logger.info(f"Connectivity checking: ENABLED (will retry up to {config.max_retries} times with {config.retry_interval}s interval)")
        else:
            logger.info("Connectivity checking: DISABLED (--skip-connectivity-test enabled)")
        
        executor = CommandExecutor(config)
        prepare_machine(config, executor)
        logger.info("Machine preparation completed successfully")
        logger.info("FIO and dependencies are now installed on all hosts")
        logger.info("You can now run the full test suite without --prepare-machine")
        return 0
    
    # Confirmation prompt
    if not config.skip_confirmation:
        print("\n")
        logger.warning("WARNING: This script will format storage devices on all hosts!")
        logger.warning(f"Hosts: {' '.join(config.vm_hosts)}")
        logger.warning("Devices to be formatted:")
        for host in config.vm_hosts:
            device = config.storage_devices.get(host, "N/A")
            logger.warning(f"  {host}: /dev/{device}")
        print("\n")
        confirm = input("Are you sure you want to continue? (yes/no): ")
        if confirm != "yes":
            logger.info("Operation cancelled by user")
            return 0
    
    # Initialize executor
    executor = CommandExecutor(config)
    
    # Prepare storage
    prepare_storage(config, executor)
    
    # Write test data
    write_test_data(config, executor)
    
    # Run FIO tests
    run_fio_tests(config, executor)
    
    # Collect results
    results_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    machine_count = len(config.vm_hosts)
    sanitized_desc = re.sub(r'[^a-z0-9]', '_', config.description.lower()) if config.description else ""
    sanitized_desc = re.sub(r'_+', '_', sanitized_desc).strip('_')
    
    if sanitized_desc:
        results_dir = f"./fio-results-{results_timestamp}-{sanitized_desc}-machines_{machine_count}"
    else:
        results_dir = f"./fio-results-{results_timestamp}-machines_{machine_count}"
    
    collect_results(config, executor, results_dir)
    
    # Cleanup
    cleanup_storage(config, executor)
    
    logger.info("FIO performance testing completed successfully")
    logger.info(f"Results have been copied to localhost: {results_dir}")
    return 0


def prepare_machine(config: FioTestConfig, executor: CommandExecutor) -> None:
    """Prepare machines by installing FIO dependencies only"""
    logger.info("Preparing machines - installing FIO dependencies only...")
    
    # Check if FIO is already installed on each host
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            # Format command as a single line with proper bash -c wrapping
            # This ensures multi-line commands are executed correctly via SSH
            # Use single quotes for outer command to avoid quote conflicts
            cmd = (
                "bash -c '"
                "if command -v fio &> /dev/null; then "
                "echo \"FIO is already installed on this host\"; "
                "fio --version; "
                "else "
                "echo \"Installing FIO and dependencies...\"; "
                "dnf install -y fio xfsprogs util-linux; "
                "echo \"FIO installation completed\"; "
                "fio --version; "
                "fi"
                "'"
            )
            future = pool.submit(executor.execute_command, host, cmd, "Checking and installing FIO dependencies")
            futures.append(future)
        
        # Wait for all installations to complete
        failed = 0
        for future in as_completed(futures):
            success, output = future.result()
            if not success:
                logger.error(f"Failed to install FIO dependencies: {output}")
                failed += 1
            else:
                # Log output to show what happened
                if output:
                    logger.info(f"Installation output: {output.strip()}")
        
        if failed > 0:
            logger.error(f"{failed}/{len(config.vm_hosts)} hosts failed to install FIO dependencies")
            sys.exit(1)
    
    logger.info("Machine preparation completed - FIO dependencies are ready on all hosts")


def prepare_storage(config: FioTestConfig, executor: CommandExecutor) -> None:
    """Prepare storage on all VMs"""
    logger.info("Preparing storage on VMs with parallel execution...")
    
    # Step 1: Create directories
    logger.info("Step 1/5: Creating test directories on all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            cmd = f"mkdir -p {config.output_dir} {config.mount_point}"
            future = pool.submit(executor.execute_command, host, cmd, "Creating test directories")
            futures.append(future)
        for future in as_completed(futures):
            success, output = future.result()
            if not success:
                logger.error(f"Failed to create directories: {output}")
    
    # Step 2: Validate devices
    logger.info("Step 2/5: Validating test devices on all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            device = config.storage_devices[host]
            cmd = f"test -b /dev/{device} && echo 'Found block device /dev/{device}' && lsblk /dev/{device} || (echo 'ERROR: Block device /dev/{device} not found' && exit 1)"
            future = pool.submit(executor.execute_command, host, cmd, "Validating test device")
            futures.append(future)
        for future in as_completed(futures):
            success, output = future.result()
            if not success:
                logger.error(f"Device validation failed: {output}")
                sys.exit(1)
    
    # Step 3: Unmount existing mounts
    logger.info("Step 3/5: Unmounting existing mounts on all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            cmd = f"mountpoint -q {config.mount_point} && (echo 'Unmounting {config.mount_point}' && umount {config.mount_point} || true) || echo 'Mount point {config.mount_point} is not mounted'"
            future = pool.submit(executor.execute_command, host, cmd, "Unmounting existing mount")
            futures.append(future)
        for future in as_completed(futures):
            future.result()  # Don't fail on unmount errors
    
    # Step 4: Format devices
    logger.info("Step 4/5: Formatting devices on all hosts (WARNING: destructive operation)...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            device = config.storage_devices[host]
            cmd = f"echo 'WARNING: Formatting /dev/{device} with {config.filesystem}' && mkfs.{config.filesystem} -f /dev/{device}"
            future = pool.submit(executor.execute_command, host, cmd, "Formatting test device")
            futures.append(future)
        for future in as_completed(futures):
            success, output = future.result()
            if not success:
                logger.error(f"Formatting failed: {output}")
                sys.exit(1)
    
    # Step 5: Mount devices
    logger.info("Step 5/5: Mounting devices on all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            device = config.storage_devices[host]
            cmd = f"mount /dev/{device} {config.mount_point}"
            future = pool.submit(executor.execute_command, host, cmd, "Mounting test device")
            futures.append(future)
        for future in as_completed(futures):
            success, output = future.result()
            if not success:
                logger.error(f"Mounting failed: {output}")
                sys.exit(1)
    
    logger.info("Storage preparation completed on all hosts!")


def write_test_data(config: FioTestConfig, executor: CommandExecutor) -> None:
    """Write initial test dataset"""
    logger.info("Writing initial test dataset...")
    
    fio_cmd = (
        f"cd {config.output_dir} && fio "
        f"--name=testfile "
        f"--directory={config.mount_point} "
        f"--size={config.test_size} "
        f"--rw=randwrite "
        f"--bs=4k "
        f"--runtime={config.test_runtime} "
        f"--direct={config.direct_io} "
        f"--numjobs={config.numjobs} "
        f"--time_based=1 "
        f"--iodepth={config.iodepth} "
        f"--output-format={config.output_format} "
        f"--output=write_dataset.json"
    )
    
    # Start FIO processes on all hosts
    threads = []
    for host in config.vm_hosts:
        thread = executor.execute_background(host, fio_cmd, "Writing test dataset")
        threads.append(thread)
    
    # Wait for all threads to start (they just start the FIO process)
    for thread in threads:
        thread.join(timeout=10)  # Wait for thread to start the process
    
    # Now wait for FIO processes to actually complete
    logger.info("Waiting for FIO dataset writing to complete on all hosts...")
    # Use actual test runtime from config (with buffer)
    expected_runtime = int(config.test_runtime) if config.test_runtime else 300
    start_time = time.time()
    check_interval = 10  # Check every 10 seconds
    
    while True:
        all_done = True
        running_count = 0
        
        for host in config.vm_hosts:
            if executor.check_task_running(host, "fio.*testfile"):
                all_done = False
                running_count += 1
        
        if all_done:
            logger.info("All FIO dataset writing processes completed")
            break
        
        elapsed = time.time() - start_time
        if elapsed > expected_runtime + 60:  # Add 60s buffer
            logger.warning(f"FIO dataset writing exceeded expected time ({expected_runtime}s)")
            logger.warning(f"{running_count} hosts still have FIO processes running")
            break
        
        logger.info(f"Waiting for FIO dataset writing... ({running_count} hosts still running, {int(elapsed)}s elapsed)")
        time.sleep(check_interval)
    
    logger.info("Test dataset writing completed")


def migrate_vms_during_test(config: FioTestConfig, pattern: str) -> bool:
    """Migrate VMs during FIO test"""
    if not config.migrate_workloads or pattern not in config.migrate_workloads:
        return True
    
    if config.use_virtctl is False:
        logger.warning(f"Migration requested for pattern '{pattern}' but SSH-only mode is enabled")
        return True
    
    if not config.namespace or config.namespace == "N/A":
        logger.warning(f"Migration requested for pattern '{pattern}' but namespace is not set")
        return True
    
    # Get VMs to migrate
    executor = CommandExecutor(config)
    vms_to_migrate = [h for h in config.vm_hosts if executor.is_vm_host(h)]
    
    if not vms_to_migrate:
        logger.info(f"No VMs found to migrate for pattern '{pattern}'")
        return True
    
    if config.migrate_interval > 0:
        logger.info(f"Starting VM migrations for pattern '{pattern}' ({len(vms_to_migrate)} VMs, sequential with {config.migrate_interval}s interval)...")
        failed_vms = []
        
        # First attempt: migrate all VMs
        for vm in vms_to_migrate:
            logger.info(f"Migrating VM: {vm}")
            try:
                result = subprocess.run(
                    ["virtctl", "-n", config.namespace, "migrate", vm],
                    capture_output=True,
                    timeout=600
                )
                if result.returncode == 0:
                    logger.info(f"✓ Successfully migrated VM: {vm}")
                else:
                    logger.error(f"✗ Failed to migrate VM: {vm}")
                    if result.stderr:
                        logger.error(f"  Error: {result.stderr.decode() if isinstance(result.stderr, bytes) else result.stderr}")
                    failed_vms.append(vm)
                
                if vm != vms_to_migrate[-1]:
                    time.sleep(config.migrate_interval)
            except Exception as e:
                logger.error(f"✗ Failed to migrate VM: {vm} - {e}")
                failed_vms.append(vm)
        
        # Retry failed migrations
        if failed_vms:
            logger.info(f"Retrying {len(failed_vms)} failed VM migrations: {', '.join(failed_vms)}")
            retry_failed = []
            for vm in failed_vms:
                logger.info(f"Retrying migration for VM: {vm}")
                try:
                    result = subprocess.run(
                        ["virtctl", "-n", config.namespace, "migrate", vm],
                        capture_output=True,
                        timeout=600
                    )
                    if result.returncode == 0:
                        logger.info(f"✓ Successfully migrated VM: {vm} (retry)")
                    else:
                        logger.error(f"✗ Failed to migrate VM: {vm} (retry)")
                        if result.stderr:
                            logger.error(f"  Error: {result.stderr.decode() if isinstance(result.stderr, bytes) else result.stderr}")
                        retry_failed.append(vm)
                    
                    if vm != failed_vms[-1]:
                        time.sleep(config.migrate_interval)
                except Exception as e:
                    logger.error(f"✗ Failed to migrate VM: {vm} (retry) - {e}")
                    retry_failed.append(vm)
            
            if retry_failed:
                logger.error(f"{len(retry_failed)}/{len(vms_to_migrate)} VM migrations failed after retry: {', '.join(retry_failed)}")
                return False
            else:
                logger.info(f"All failed migrations succeeded on retry")
                logger.info(f"All VM migrations completed successfully for pattern '{pattern}' (after retry)")
                return True
        
        logger.info(f"All VM migrations completed successfully for pattern '{pattern}'")
        return True
    else:
        logger.info(f"Starting VM migrations for pattern '{pattern}' ({len(vms_to_migrate)} VMs, parallel)...")
        
        def migrate_vm(vm_name):
            """Migrate a single VM and return (success, vm_name)"""
            logger.info(f"Migrating VM: {vm_name}")
            try:
                result = subprocess.run(
                    ["virtctl", "-n", config.namespace, "migrate", vm_name],
                    capture_output=True,
                    timeout=600
                )
                if result.returncode == 0:
                    logger.info(f"✓ Successfully migrated VM: {vm_name}")
                    return True, vm_name
                else:
                    logger.error(f"✗ Failed to migrate VM: {vm_name}")
                    if result.stderr:
                        logger.error(f"  Error: {result.stderr.decode() if isinstance(result.stderr, bytes) else result.stderr}")
                    return False, vm_name
            except Exception as e:
                logger.error(f"✗ Failed to migrate VM: {vm_name} - {e}")
                return False, vm_name
        
        # First attempt: migrate all VMs in parallel
        with ThreadPoolExecutor(max_workers=len(vms_to_migrate)) as pool:
            futures = [pool.submit(migrate_vm, vm) for vm in vms_to_migrate]
            failed_vms = []
            for future in as_completed(futures):
                success, vm_name = future.result()
                if not success:
                    failed_vms.append(vm_name)
        
        # Retry failed migrations
        if failed_vms:
            logger.info(f"Retrying {len(failed_vms)} failed VM migrations in parallel: {', '.join(failed_vms)}")
            with ThreadPoolExecutor(max_workers=len(failed_vms)) as pool:
                futures = [pool.submit(migrate_vm, vm) for vm in failed_vms]
                retry_failed = []
                for future in as_completed(futures):
                    success, vm_name = future.result()
                    if not success:
                        retry_failed.append(vm_name)
                    else:
                        logger.info(f"✓ Successfully migrated VM: {vm_name} (retry)")
            
            if retry_failed:
                logger.error(f"{len(retry_failed)}/{len(vms_to_migrate)} VM migrations failed after retry: {', '.join(retry_failed)}")
                return False
            else:
                logger.info(f"All failed migrations succeeded on retry")
                logger.info(f"All VM migrations completed successfully for pattern '{pattern}' (after retry)")
                return True
        
        logger.info(f"All VM migrations completed successfully for pattern '{pattern}'")
        return True


def run_fio_tests(config: FioTestConfig, executor: CommandExecutor) -> None:
    """Run FIO performance tests"""
    logger.info("Running FIO performance tests...")
    
    test_counter = 1
    
    for bs in config.block_sizes:
        logger.info(f"Starting block size iteration: {bs}")
        
        for pattern in config.io_patterns:
            logger.info(f"Running test {test_counter}: {pattern} with block size {bs}")
            
            # Build FIO command
            fio_cmd = (
                f"cd {config.output_dir} && fio "
                f"--name=testfile "
                f"--directory={config.mount_point} "
                f"--size={config.test_size} "
                f"--rw={pattern} "
                f"--bs={bs} "
                f"--runtime={config.test_runtime} "
                f"--direct={config.direct_io} "
                f"--numjobs={config.numjobs} "
                f"--time_based=1 "
                f"--iodepth={config.iodepth} "
                f"--output-format={config.output_format}"
            )
            
            if config.rate_iops:
                fio_cmd += f" --rate_iops={config.rate_iops}"
            
            test_name = f"fio-test-{pattern}-bs-{bs}"
            fio_cmd += f" --output={test_name}.json"
            
            # Start FIO tests on all hosts
            threads = []
            for host in config.vm_hosts:
                logger.info(f"Starting FIO test on {host}: {test_name}")
                thread = executor.execute_background(host, fio_cmd, f"FIO test: {pattern}, block size: {bs}")
                threads.append(thread)
            
            # Check if migration is needed
            if pattern in config.migrate_workloads:
                # Ensure test_runtime is an integer
                test_runtime_int = int(config.test_runtime) if config.test_runtime else 0
                half_runtime = test_runtime_int // 2
                logger.info(f"Migration configured for pattern '{pattern}' - will migrate VMs at {half_runtime}s (midpoint of {test_runtime_int}s runtime)")
                logger.info(f"Waiting {half_runtime}s before triggering VM migrations...")
                time.sleep(half_runtime)
                
                logger.info("Triggering VM migrations at midpoint of test runtime...")
                migrate_vms_during_test(config, pattern)
            
            # Wait for all threads to start (they just start the FIO process)
            for thread in threads:
                thread.join(timeout=10)  # Wait for thread to start the process
            
            # Now wait for FIO processes to actually complete
            logger.info(f"Waiting for all FIO tests to complete for {pattern} with block size {bs}...")
            # Ensure test_runtime is an integer for timeout calculation
            test_runtime_int = int(config.test_runtime) if config.test_runtime else 0
            start_time = time.time()
            check_interval = 10  # Check every 10 seconds
            
            while True:
                all_done = True
                running_count = 0
                check_failures = 0
                
                for host in config.vm_hosts:
                    # Check if FIO process is still running for this specific test
                    # Use the test name pattern to identify the correct FIO process
                    try:
                        if executor.check_task_running(host, f"fio.*{test_name}"):
                            all_done = False
                            running_count += 1
                    except Exception as e:
                        # If check fails (timeout, connection error, etc.), don't fail the whole test
                        # Just log and continue - we'll verify with result files later
                        check_failures += 1
                        logger.debug(f"Failed to check task status on {host}: {e}")
                        # Assume not running if check fails (fail-safe)
                        pass
                
                if all_done:
                    logger.info("All FIO test processes completed")
                    break
                
                elapsed = time.time() - start_time
                if elapsed > test_runtime_int + 60:  # Add 60s buffer
                    logger.warning(f"FIO test exceeded expected time ({test_runtime_int}s)")
                    logger.warning(f"{running_count} hosts still have FIO processes running")
                    # Check if result files exist - if they do, the test likely completed
                    result_files_exist = 0
                    for host in config.vm_hosts:
                        check_cmd = f"test -f {config.output_dir}/{test_name}.json && echo 'exists' || echo 'missing'"
                        # Use short timeout for quick file check
                        success, output = executor.execute_command(host, check_cmd, "Checking result file", max_retries=1, retry_interval=1, timeout=30)
                        if success and "exists" in output:
                            result_files_exist += 1
                    
                    if result_files_exist == len(config.vm_hosts):
                        logger.info(f"All result files exist - test completed successfully despite timeout warnings")
                        break
                    else:
                        logger.warning(f"Only {result_files_exist}/{len(config.vm_hosts)} result files exist")
                        break
                
                logger.info(f"Waiting for FIO tests... ({running_count} hosts still running, {int(elapsed)}s elapsed)")
                time.sleep(check_interval)
            
            test_counter += 1
            logger.info(f"Completed test {test_counter - 1}: {pattern} with block size {bs}")
    
    logger.info("Completed all FIO performance tests")


def collect_results(config: FioTestConfig, executor: CommandExecutor, results_dir: str) -> None:
    """Collect test results from all hosts"""
    logger.info(f"Collecting test results in parallel from {len(config.vm_hosts)} hosts...")
    os.makedirs(results_dir, exist_ok=True)
    
    # Pre-create host directories
    for host in config.vm_hosts:
        host_dir = os.path.join(results_dir, host)
        os.makedirs(host_dir, exist_ok=True)
    
    # Create archives on VMs
    logger.info("Creating results archives on all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            cmd = f"cd {config.output_dir} && tar czf fio-results.tar.gz *.json"
            future = pool.submit(executor.execute_command, host, cmd, f"Creating results archive for {host}")
            futures.append(future)
        for future in as_completed(futures):
            future.result()
    
    # Copy results from VMs
    logger.info("Copying results from all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            host_dir = os.path.join(results_dir, host)
            source = f"root@vmi/{host}:{config.output_dir}/fio-results.tar.gz"
            destination = os.path.join(host_dir, "fio-results.tar.gz")
            
            def copy_results(host_name, src, dst, host_d):
                try:
                    scp_cmd = executor.get_scp_command(src, dst)
                    result = subprocess.run(scp_cmd, capture_output=True, timeout=300)
                    if result.returncode == 0:
                        logger.info(f"Successfully copied results from {host_name}")
                        # Extract results
                        try:
                            with tarfile.open(dst, 'r:gz') as tar:
                                # Use secure extraction to avoid CVE-2007-4559
                                # Filter members to only allow safe paths (no absolute/parent paths)
                                safe_members = []
                                for member in tar.getmembers():
                                    # Normalize the path and remove leading slashes
                                    safe_name = member.name.lstrip('/')
                                    safe_name = os.path.normpath(safe_name)
                                    
                                    # Prevent directory traversal attacks
                                    if safe_name.startswith('..') or os.path.isabs(safe_name):
                                        logger.warning(f"Skipping unsafe path in tar: {member.name}")
                                        continue
                                    
                                    # Create a new member with the safe name
                                    member.name = safe_name
                                    safe_members.append(member)
                                
                                # Extract with filtered members
                                tar.extractall(host_d, members=safe_members)
                            os.remove(dst)
                            logger.info(f"Extracted results for {host_name}")
                        except Exception as e:
                            logger.warning(f"Failed to extract results for {host_name}: {e}")
                    else:
                        logger.error(f"Failed to copy results from {host_name}")
                except Exception as e:
                    logger.error(f"Error copying results from {host_name}: {e}")
            
            futures.append(pool.submit(copy_results, host, source, destination, host_dir))
        
        for future in as_completed(futures):
            future.result()
    
    logger.info(f"All results collected in: {results_dir}")


def cleanup_storage(config: FioTestConfig, executor: CommandExecutor) -> None:
    """Clean up storage on VMs"""
    logger.info("Cleaning up storage on VMs...")
    
    # Unmount mount points
    logger.info("Step 1/3: Cleaning up storage mount points on all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            cmd = f"mountpoint -q {config.mount_point} && (umount {config.mount_point} && echo 'Successfully unmounted {config.mount_point}') || echo 'Mount point {config.mount_point} is not mounted'"
            future = pool.submit(executor.execute_command, host, cmd, "Cleaning up storage mount points")
            futures.append(future)
        for future in as_completed(futures):
            future.result()
    
    # Clean up test results
    logger.info("Step 2/3: Cleaning up test results on all hosts...")
    with ThreadPoolExecutor(max_workers=len(config.vm_hosts)) as pool:
        futures = []
        for host in config.vm_hosts:
            cmd = f"rm -rf {config.output_dir}/*.json 2>/dev/null || true && echo 'Test results cleanup completed'"
            future = pool.submit(executor.execute_command, host, cmd, "Cleaning up test results")
            futures.append(future)
        for future in as_completed(futures):
            future.result()
    
    logger.info("Storage cleanup completed")


if __name__ == "__main__":
    sys.exit(main())

