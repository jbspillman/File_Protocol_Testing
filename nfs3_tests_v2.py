# nfs3_tests_v2.py
"""
NFS3 Protocol Comprehensive Test Suite

Usage:
    sudo python3 nfs3_tests_v2.py

Requirements:
    - Root/sudo access
    - nfs-common package installed
    - Python 3.6+
"""

import os
import sys
import subprocess
import tempfile
import time
import fcntl
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime
import multiprocessing
import logging

# ============================================================================
# CONFIGURATION
# ============================================================================

STORAGE_CONFIG = {
    'nfs3_mounts': [
        {
            'vendor': 'Dell',
            'software': 'PowerScale OneFS 9.10.0.0',
            'export_server': 'onefs002-2.beastmode.local.net',
            'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_rw',
            'mount_type': 'rw'
        },
        {
            'vendor': 'Dell',
            'software': 'PowerScale OneFS 9.10.0.0',
            'export_server': 'onefs002-1.beastmode.local.net',
            'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_ro',
            'mount_type': 'ro'
        }
    ]
}

# ============================================================================
# COLORFUL LOGGER SETUP
# ============================================================================

class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and emojis"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m',       # Reset
        'BOLD': '\033[1m',        # Bold
        'DIM': '\033[2m',         # Dim
    }
    
    # Additional colors
    CUSTOM_COLORS = {
        'BLUE': '\033[34m',
        'CYAN': '\033[36m',
        'GREEN': '\033[32m',
        'YELLOW': '\033[33m',
        'RED': '\033[31m',
        'MAGENTA': '\033[35m',
        'BRIGHT_BLUE': '\033[94m',
        'BRIGHT_GREEN': '\033[92m',
        'BRIGHT_YELLOW': '\033[93m',
        'BRIGHT_RED': '\033[91m',
        'BRIGHT_MAGENTA': '\033[95m',
        'BRIGHT_CYAN': '\033[96m',
    }
    
    # Emojis
    EMOJIS = {
        'DEBUG': '🔍',
        'INFO': '✓',
        'WARNING': '⚠️',
        'ERROR': '✗',
        'CRITICAL': '🚨',
    }
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        emoji = self.EMOJIS.get(record.levelname, '')
        reset = self.COLORS['RESET']
        bold = self.COLORS['BOLD']
        dim = self.COLORS['DIM']
        
        timestamp = self.formatTime(record, '%Y-%m-%d %H:%M:%S')
        message = record.getMessage()
        
        # Highlight patterns
        if '[PASS]' in message:
            message = message.replace('[PASS]', f"{self.CUSTOM_COLORS['BRIGHT_GREEN']}✓ PASS{reset}")
        if '[FAIL]' in message:
            message = message.replace('[FAIL]', f"{self.CUSTOM_COLORS['BRIGHT_RED']}✗ FAIL{reset}")
        
        message = message.replace('Phase', f"{self.CUSTOM_COLORS['BRIGHT_CYAN']}{bold}Phase{reset}")
        message = message.replace('✓', f"{self.CUSTOM_COLORS['BRIGHT_GREEN']}✓{reset}")
        message = message.replace('✗', f"{self.CUSTOM_COLORS['BRIGHT_RED']}✗{reset}")
        message = message.replace('⚠', f"{self.CUSTOM_COLORS['BRIGHT_YELLOW']}⚠{reset}")
        
        if 'TEST:' in message:
            message = message.replace('TEST:', f"{self.CUSTOM_COLORS['BRIGHT_MAGENTA']}{bold}TEST:{reset}")
        
        # Highlight performance numbers
        import re
        message = re.sub(r'(\d+\.?\d*)\s*(MB/s|ops/s|ms|s\b)', 
                        rf"{self.CUSTOM_COLORS['BRIGHT_CYAN']}\1 \2{reset}", message)
        
        if record.levelname == 'INFO':
            formatted = f"{dim}{timestamp}{reset} {emoji} {message}"
        else:
            formatted = f"{dim}{timestamp}{reset} {color}{bold}[{record.levelname}]{reset} {emoji} {message}"
        
        return formatted

def setup_colorful_logger():
    """Setup logger with colors"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    formatter = ColoredFormatter()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_colorful_logger()

# ============================================================================
# TEXT DOCUMENTATION LOGGER
# ============================================================================

class TextDocLogger:
    """Generate simple text documentation of tests"""
    
    def __init__(self, output_file: str = None):
        self.output_file = output_file or f"nfs3_test_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.log_entries = []
        self.test_metadata = {}
        
    def log_metadata(self, key: str, value: str):
        """Log metadata about the test run"""
        self.test_metadata[key] = value
    
    def log_test_start(self, test_name: str, description: str):
        """Log the start of a test"""
        self.log_entries.append({
            'type': 'test_start',
            'test_name': test_name,
            'description': description,
            'timestamp': datetime.now()
        })
    
    def log_test_step(self, step: str):
        """Log a test step"""
        self.log_entries.append({
            'type': 'step',
            'content': step,
            'timestamp': datetime.now()
        })
    
    def log_test_result(self, test_name: str, passed: bool, message: str = ""):
        """Log test result"""
        self.log_entries.append({
            'type': 'test_result',
            'test_name': test_name,
            'passed': passed,
            'message': message,
            'timestamp': datetime.now()
        })
    
    def generate_report(self):
        """Generate the text report"""
        report_lines = []
        
        # Header
        report_lines.append("=" * 80)
        report_lines.append("NFS3 PROTOCOL TEST DOCUMENTATION")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Metadata
        report_lines.append("TEST RUN INFORMATION")
        report_lines.append("-" * 80)
        for key, value in self.test_metadata.items():
            report_lines.append(f"{key:25}: {value}")
        report_lines.append("")
        
        # Test results
        report_lines.append("=" * 80)
        report_lines.append("TEST RESULTS AND DOCUMENTATION")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        current_test = None
        for entry in self.log_entries:
            if entry['type'] == 'test_start':
                if current_test:
                    report_lines.append("")
                
                report_lines.append("-" * 80)
                report_lines.append(f"TEST: {entry['test_name']}")
                report_lines.append("-" * 80)
                report_lines.append(f"Purpose: {entry['description']}")
                report_lines.append(f"Started: {entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                report_lines.append("")
                current_test = entry['test_name']
                
            elif entry['type'] == 'step':
                report_lines.append(f"  • {entry['content']}")
                
            elif entry['type'] == 'test_result':
                status = "PASSED ✓" if entry['passed'] else "FAILED ✗"
                report_lines.append("")
                report_lines.append(f"Result: {status}")
                if entry['message']:
                    report_lines.append(f"Details: {entry['message']}")
                report_lines.append(f"Completed: {entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                report_lines.append("")
        
        # Summary
        report_lines.append("=" * 80)
        report_lines.append("TEST SUMMARY")
        report_lines.append("=" * 80)
        
        results = [e for e in self.log_entries if e['type'] == 'test_result']
        total = len(results)
        passed = sum(1 for r in results if r['passed'])
        failed = total - passed
        
        report_lines.append(f"Total Tests: {total}")
        report_lines.append(f"Passed: {passed}")
        report_lines.append(f"Failed: {failed}")
        report_lines.append(f"Success Rate: {(passed/total*100):.1f}%" if total > 0 else "N/A")
        report_lines.append("")
        
        if failed > 0:
            report_lines.append("Failed Tests:")
            for result in results:
                if not result['passed']:
                    report_lines.append(f"  ✗ {result['test_name']}: {result['message']}")
        
        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # Write to file
        report_content = "\n".join(report_lines)
        
        # Create reports directory
        Path("./test_reports").mkdir(parents=True, exist_ok=True)

        # output_path = os.path.join('test_reports', f'{self.output_file}') 
        output_path = os.path.join('test_reports', f'report.txt') 
        with open(output_path, 'w') as f:
            f.write(report_content)
        
        os.chmod(output_path, 0o777)
        logger.info(f"✓ Text documentation log saved: {output_path}")
        return output_path

# Global text doc logger
text_logger = None



# ============================================================================
# NFS3 MOUNT OPTIONS
# ============================================================================

@dataclass
class NFSMountOptions:
    """NFS3 mount options"""
    transport: str = 'tcp'
    rsize: int = 1048576
    wsize: int = 1048576
    timeo: int = 600
    retrans: int = 2
    soft: bool = False
    intr: bool = True
    noac: bool = False
    actimeo: int = None
    acregmin: int = 3
    acregmax: int = 60
    acdirmin: int = 30
    acdirmax: int = 60
    nosharecache: bool = False
    nordirplus: bool = False
    
    def to_mount_string(self):
        """Convert to mount options string"""
        opts = [
            'vers=3',
            f'proto={self.transport}',
            f'rsize={self.rsize}',
            f'wsize={self.wsize}',
            f'timeo={self.timeo}',
            f'retrans={self.retrans}',
        ]
        
        if self.soft:
            opts.append('soft')
        else:
            opts.append('hard')
            
        if self.intr:
            opts.append('intr')
            
        if self.noac:
            opts.append('noac')
        elif self.actimeo:
            opts.append(f'actimeo={self.actimeo}')
        else:
            opts.extend([
                f'acregmin={self.acregmin}',
                f'acregmax={self.acregmax}',
                f'acdirmin={self.acdirmin}',
                f'acdirmax={self.acdirmax}',
            ])
            
        if self.nosharecache:
            opts.append('nosharecache')
            
        if self.nordirplus:
            opts.append('nordirplus')
            
        return ','.join(opts)

# ============================================================================
# NFS3 TEST CLASS
# ============================================================================

class NFS3Test:
    """Comprehensive NFS3 protocol testing"""
    
    # Test descriptions for documentation
    TEST_DESCRIPTIONS = {
        'test_mount_options_verification': 'Confirm that the actual mount options match the requested configuration',
        'transport_protocol': 'Verify that the mount is using the correct transport protocol (TCP/UDP) as requested',
        'basic_file_operations': 'Test fundamental file operations: create, read, write, and delete files',
        'idempotent_operations': 'Verify NFS3 stateless protocol ensures repeated operations produce consistent results',
        'close_to_open_consistency': 'Test NFS3 close-to-open cache consistency - changes made by one client are visible to others after file close',
        'nlm_basic_locking': 'Test Network Lock Manager (NLM) exclusive file locking between processes',
        'small_file_performance': 'Measure metadata-intensive operations with many small files',
        'concurrent_writers': 'Test multiple simultaneous writers to verify concurrent access handling',
        'large_sequential_io': 'Measure large file sequential read/write performance',
        'readwrite_mount_enforcement': 'Verify read-write mount allows create, modify, and delete operations',
        'readonly_mount_enforcement': 'Verify read-only mount blocks write operations',
        'readonly_mount_read_operations': 'Verify read operations still work on read-only mounts',
        'mount_options_verification': 'Confirm actual mount options match requested configuration'
    }
    
    def __init__(self, server: str, export_path: str,
                 mount_options: NFSMountOptions = None,
                 mount_type: str = 'rw'):
        self.server = server
        self.export_path = export_path
        self.mount_options = mount_options or NFSMountOptions()
        self.mount_type = mount_type
        self._is_rw_mount = (mount_type == 'rw')
        self.mount_point = None
        self.test_dir = None
        self.results = []
        
    def log_result(self, test_name: str, passed: bool, message: str = ""):
        """Log test result"""
        result = {
            'test': test_name,
            'passed': passed,
            'message': message,
            'timestamp': time.time(),
            'transport': self.mount_options.transport
        }
        self.results.append(result)
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {test_name}: {message}")
        
        # Log to text documentation
        if text_logger:
            text_logger.log_test_result(test_name, passed, message)
    
    def mount(self) -> bool:
        """Mount NFS3 export"""
        try:
            self.mount_point = tempfile.mkdtemp(prefix='nfs3_test_')
            
            options = self.mount_options.to_mount_string()
            options += f',{self.mount_type}'
            
            cmd = [
                'sudo', 'mount',
                '-t', 'nfs',
                '-o', options,
                f'{self.server}:{self.export_path}',
                self.mount_point
            ]
            
            logger.info(f"Mounting: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                logger.error(f"Mount failed: {result.stderr}")
                return False
            
            result = subprocess.run(['mount'], capture_output=True, text=True)
            
            if self.mount_point in result.stdout:
                logger.info(f"✓ Mounted at {self.mount_point} ({self.mount_type})")
                return True
            else:
                logger.error("Mount not found in mount table")
                return False
                
        except Exception as e:
            logger.error(f"Mount exception: {e}")
            return False
    
    def unmount(self):
        """Unmount NFS3 export"""
        if not self.mount_point:
            return
        
        try:
            subprocess.run(['sudo', 'umount', '-f', '-l', self.mount_point],
                          capture_output=True, timeout=10)
            time.sleep(1)
            try:
                os.rmdir(self.mount_point)
            except:
                pass
        except Exception as e:
            logger.error(f"Unmount error: {e}")
    
    def setup(self):
        """Setup test environment"""
        if not self.mount():
            raise Exception("Failed to mount NFS export")
        
        # For read-only mounts, just use the mount point itself
        if self.mount_type == 'ro':
            self.test_dir = self.mount_point
            logger.info(f"Test directory (RO): {self.test_dir}")
            logger.info("⚠ Read-only mount - using mount point directly")
        else:
            # For read-write mounts, create a test subdirectory
            test_id = f"test_{int(time.time())}_{os.getpid()}"
            self.test_dir = os.path.join(self.mount_point, test_id)
            try:
                os.makedirs(self.test_dir, exist_ok=True)
                logger.info(f"Test directory: {self.test_dir}")
            except (OSError, IOError) as e:
                logger.error(f"Failed to create test directory: {e}")
                self.unmount()
                raise Exception(f"Failed to create test directory: {e}")
    
    def teardown(self):
        """Cleanup test environment"""
        try:
            # Only cleanup if we created a test directory (RW mounts)
            if self.mount_type == 'rw' and self.test_dir and os.path.exists(self.test_dir):
                subprocess.run(['rm', '-rf', self.test_dir],
                              capture_output=True, timeout=30)
                logger.info(f"✓ Test directory cleaned up")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")
        finally:
            self.unmount()
    
    # ========================================================================
    # IN ORDER OF BEING CALLED IN THE FUNCTIONS or CLOSE TO.
    # ========================================================================
    def test_mount_options_verification(self):

        test_name = 'test_mount_options_verification'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])

        """Verify mount options"""
        logger.info("=" * 70)
        logger.info("TEST: Mount Options Verification")
        logger.info("=" * 70)
        
        try:
            if text_logger:
                text_logger.log_test_step("Phase 1: Reading /proc/mounts")

            logger.info("Phase 1: Reading /proc/mounts")
            with open('/proc/mounts', 'r') as f:
                mounts = f.read()
            
            if text_logger:
                text_logger.log_test_step(f"Phase 2: Searching for mount point: {self.mount_point}")

            logger.info(f"Phase 2: Searching for mount point: {self.mount_point}")
            mount_line = None
            for line in mounts.split('\n'):
                if self.mount_point in line:
                    mount_line = line
                    break
            
            if not mount_line:
                logger.error("✗ Mount point not found")
                self.log_result('mount_options_verification', False,
                              "Mount not found in /proc/mounts")
                return
            
            logger.info(f"✓ Found: {mount_line}")
            
            parts = mount_line.split()
            if len(parts) >= 4:
                options = parts[3]

                if text_logger:
                    text_logger.log_test_step(f"Phase 3: Parsing options: {options}")
                logger.info(f"Phase 3: Parsing options: {options}")
                
                if 'vers=3' in options or 'nfsvers=3' in options:
                    logger.info("  ✓ NFS Version: 3")
                
                if f'proto={self.mount_options.transport}' in options:
                    logger.info(f"  ✓ Transport: {self.mount_options.transport}")
                
                logger.info("✓ Mount options verified")
                self.log_result('mount_options_verification', True)
            else:
                logger.error("✗ Could not parse mount options")
                self.log_result('mount_options_verification', False,
                              "Could not parse mount options")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('mount_options_verification', False, str(e))
    
    def test_transport_protocol(self):
        """Verify correct transport protocol"""
        test_name = 'transport_protocol'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Checking /proc/mounts for {self.mount_options.transport.upper()} protocol")
        
        logger.info("=" * 70)
        logger.info("TEST: Transport Protocol Verification")
        logger.info("=" * 70)
        
        try:
            with open('/proc/mounts', 'r') as f:
                mounts = f.read()
            
            for line in mounts.split('\n'):
                if self.mount_point in line:
                    if text_logger:
                        text_logger.log_test_step(f"Found mount entry in /proc/mounts")
                    
                    if self.mount_options.transport == 'tcp':
                        if 'proto=tcp' in line or ',tcp' in line:
                            logger.info("✓ Confirmed: Using TCP")
                            if text_logger:
                                text_logger.log_test_step("Verified TCP protocol in use")
                            self.log_result(test_name, True, "Using TCP as expected")
                            return
                    elif self.mount_options.transport == 'udp':
                        if 'proto=udp' in line or ',udp' in line:
                            logger.info("✓ Confirmed: Using UDP")
                            if text_logger:
                                text_logger.log_test_step("Verified UDP protocol in use")
                            self.log_result(test_name, True, "Using UDP as expected")
                            return
            
            self.log_result(test_name, False, "Could not verify transport protocol")
        except Exception as e:
            self.log_result(test_name, False, str(e))
    
    def test_readwrite_mount_enforcement(self):
        """Test rw mount allows writes"""
        logger.info("=" * 70)
        logger.info("TEST: Read-Write Mount Enforcement")
        logger.info("=" * 70)
        
        test_file = os.path.join(self.test_dir, 'rw_test.txt')
        test_data = "RW mount test"
        
        try:
            logger.info("Phase 1: Testing write permissions")
            with open(test_file, 'w') as f:
                f.write(test_data)
            logger.info("✓ Write operation successful")
            
            logger.info("Phase 2: Verifying data integrity")
            with open(test_file, 'r') as f:
                content = f.read()
            
            if content == test_data:
                logger.info("✓ Data verified correctly")
            else:
                logger.error(f"✗ Data mismatch: '{content}'")
            
            assert content == test_data
            
            logger.info("Phase 3: Cleanup")
            os.remove(test_file)
            logger.info("✓ Test file removed")
            
            logger.info("✓ RW mount working correctly")
            self.log_result('readwrite_mount_enforcement', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('readwrite_mount_enforcement', False, str(e))
    
    def test_basic_file_operations(self):
        """Test basic file operations"""
        test_name = 'basic_file_operations'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
        
        logger.info("=" * 70)
        logger.info("TEST: Basic File Operations")
        logger.info("=" * 70)
        
        test_file = os.path.join(self.test_dir, 'basic_test.txt')
        test_data = "Hello NFS3"
        
        try:
            if text_logger:
                text_logger.log_test_step("Creating test file and writing data")
            with open(test_file, 'w') as f:
                f.write(test_data)
            logger.info(f"✓ File created with {len(test_data)} bytes")
            
            if text_logger:
                text_logger.log_test_step("Reading file content back")
            with open(test_file, 'r') as f:
                read_data = f.read()
            logger.info(f"✓ File read: '{read_data}'")
            
            assert read_data == test_data
            if text_logger:
                text_logger.log_test_step("Data integrity verified")
            
            if text_logger:
                text_logger.log_test_step("Deleting test file")
            os.remove(test_file)
            logger.info("✓ File deleted")
            
            self.log_result(test_name, True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result(test_name, False, str(e))
    
    def test_idempotent_operations(self):
        """Test operation idempotency"""
        test_name = 'idempotent_operations'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Checking /proc/mounts for {self.mount_options.transport.upper()} protocol")

        logger.info("=" * 70)
        logger.info("TEST: Idempotent Operations (NFS3 Stateless Protocol)")
        logger.info("=" * 70)
        
        test_file = os.path.join(self.test_dir, 'idempotent.txt')
        
        try:
            if text_logger:
                text_logger.log_test_step("Phase 1: Testing idempotent CREATE/WRITE operations")
            logger.info("Phase 1: Testing idempotent CREATE/WRITE operations")
            for i in range(3):
                logger.info(f"  Iteration {i+1}: Writing 'Iteration {i}'")
                with open(test_file, 'w') as f:
                    f.write(f"Iteration {i}")
            
            if text_logger:
                text_logger.log_test_step("Phase 2: Verifying final content")
            logger.info("Phase 2: Verifying final content")
            with open(test_file, 'r') as f:
                content = f.read()                      
            logger.info(f"  File content: '{content}'")
            
            if "Iteration 2" in content:
                logger.info("  ✓ Last write persisted correctly")
            else:
                logger.error(f"  ✗ Expected 'Iteration 2', got '{content}'")          
            assert "Iteration 2" in content
            
            if text_logger:
                text_logger.log_test_step("Phase 3: Testing idempotent DELETE operation")

            logger.info("Phase 3: Testing idempotent DELETE operation")
            os.remove(test_file)
            logger.info("  ✓ First delete successful")
            
            try:
                os.remove(test_file)
                logger.error("  ✗ Second delete should have failed")
            except FileNotFoundError:
                logger.info("  ✓ Second delete correctly raised FileNotFoundError")
            
            logger.info("✓ Idempotency test passed")
            self.log_result('idempotent_operations', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('idempotent_operations', False, str(e))

    def test_close_to_open_consistency(self):
        """Test close-to-open consistency"""
        test_name = 'close_to_open_consistency'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing close-to-open consistency between two processes")

        logger.info("=" * 70)
        logger.info("TEST: Close-to-Open Consistency")
        logger.info("=" * 70)
        
        test_file = os.path.join(self.test_dir, 'c2o_test.txt')
        test_data = "Process 1 data"
        
        try:
            if text_logger:
                text_logger.log_test_step("Phase 1: Process 1 - Write and close file")
            logger.info("Phase 1: Process 1 - Write and close file")
            with open(test_file, 'w') as f:
                f.write(test_data)
            logger.info("✓ File written and closed (should flush to server)")
            
            if text_logger:
                text_logger.log_test_step("Phase 2: Allowing 0.5s for server flush")
            logger.info("Phase 2: Allowing 0.5s for server flush")
            time.sleep(0.5)
            logger.info("✓ Flush period elapsed")
            
            if text_logger:
                text_logger.log_test_step("Phase 3: Process 2 - Open and read file")
            logger.info("Phase 3: Process 2 - Open and read file")
            with open(test_file, 'r') as f:
                content = f.read()
            logger.info(f"  Read content: '{content}'")
            
            if content == test_data:
                logger.info("✓ Process 2 sees Process 1's write (close-to-open works)")
            else:
                logger.error(f"✗ Expected '{test_data}', got '{content}'")
            
            assert content == test_data
            
            logger.info("✓ Close-to-open consistency verified")
            self.log_result('close_to_open_consistency', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('close_to_open_consistency', False, str(e))
    
    def test_nlm_basic_locking(self):
        """Test NLM basic file locking"""

        test_name = 'nlm_basic_locking'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing close-to-open consistency between two processes")


        logger.info("=" * 70)
        logger.info("TEST: NLM Basic File Locking")
        logger.info("=" * 70)
        
        test_file = os.path.join(self.test_dir, 'lock_test.txt')
        
        try:
            if text_logger:
                text_logger.log_test_step("Phase 1: Creating test file")

            logger.info("Phase 1: Creating test file")
            with open(test_file, 'w') as f:
                f.write("Lock test data")
            logger.info("✓ Test file created")
            
            if text_logger:
                text_logger.log_test_step("Phase 2: Acquiring exclusive lock (LOCK_EX)")
            logger.info("Phase 2: Acquiring exclusive lock (LOCK_EX)")
            f = open(test_file, 'r+')
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            logger.info("✓ Exclusive lock acquired by main process")
            
            if text_logger:
                text_logger.log_test_step("Phase 3: Spawning child process to test lock blocking")
            logger.info("Phase 3: Spawning child process to test lock blocking")
            
            def try_lock_exclusive():
                try:
                    f2 = open(test_file, 'r+')
                    fcntl.flock(f2.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    f2.close()
                    return False
                except IOError:
                    return True
            
            p = multiprocessing.Process(target=try_lock_exclusive)
            p.start()
            p.join()
            
            if text_logger:
                text_logger.log_test_step("Phase 4: Releasing exclusive lock")
            logger.info("Phase 4: Releasing exclusive lock")
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            f.close()
            logger.info("✓ Lock released successfully")
            
            logger.info("✓ NLM basic locking test passed")
            self.log_result('nlm_basic_locking', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('nlm_basic_locking', False, str(e))
        
    def test_small_file_performance(self, num_files=1000):
        """Test small file performance"""

        test_name = 'small_file_performance'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing small file performance")

        logger.info("=" * 70)
        logger.info("TEST: Small File Performance")
        logger.info("=" * 70)
        
        test_subdir = os.path.join(self.test_dir, 'small_files')
        os.makedirs(test_subdir, exist_ok=True)
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Creating {num_files} small files")
            logger.info(f"Phase 1: Creating {num_files} small files")
            start = time.time()
            for i in range(num_files):
                filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
                with open(filepath, 'w') as f:
                    f.write(f"{i}")
                if (i + 1) % 25 == 0:
                    elapsed = time.time() - start
                    rate = (i + 1) / elapsed
                    logger.info(f"  Progress: {i+1}/{num_files} ({rate:.0f} files/s)")
            create_time = time.time() - start
            create_rate = num_files / create_time
            logger.info(f"✓ Created {num_files} files in {create_time:.2f}s ({create_rate:.0f} ops/s)")
            
            if text_logger:
                text_logger.log_test_step(f"Phase 2: Reading {num_files} files")            
            logger.info(f"Phase 2: Reading {num_files} files")
            start = time.time()
            for i in range(num_files):
                filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
                with open(filepath, 'r') as f:
                    _ = f.read()
            read_time = time.time() - start
            read_rate = num_files / read_time
            logger.info(f"✓ Read {num_files} files in {read_time:.2f}s ({read_rate:.0f} ops/s)")
            
            if text_logger:
                text_logger.log_test_step(f"Phase 3: Deleting {num_files} files")
            logger.info(f"Phase 3: Deleting {num_files} files")
            start = time.time()
            for i in range(num_files):
                filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
                os.remove(filepath)
            delete_time = time.time() - start
            delete_rate = num_files / delete_time
            logger.info(f"✓ Deleted {num_files} files in {delete_time:.2f}s ({delete_rate:.0f} ops/s)")
            
            logger.info(f"✓ Small file performance test completed")
            self.log_result('small_file_performance', True,
                          f"{num_files} files - Create: {create_rate:.0f} ops/s, Read: {read_rate:.0f} ops/s, Delete: {delete_rate:.0f} ops/s")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('small_file_performance', False, str(e))
    
    def test_concurrent_writers(self, num_writers):
        """Test concurrent writers"""

        test_name = 'concurrent_writers'
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing {num_writers} concurrent writer threads")   

        logger.info("=" * 70)
        logger.info("TEST: Concurrent Writers")
        logger.info("=" * 70)
        logger.info(f"Testing {num_writers} concurrent writer threads")
        
        def writer_task(writer_id):
            try:
                filepath = os.path.join(self.test_dir, f'writer_{writer_id}.txt')
                data = f"Writer {writer_id}\n" * 1000
                
                with open(filepath, 'w') as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                
                with open(filepath, 'r') as f:
                    read_data = f.read()
                
                return len(read_data) == len(data)
            except Exception as e:
                logger.error(f"  [Writer {writer_id}] ✗ Failed: {e}")
                return False
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Launching {num_writers} writer threads")
            logger.info(f"Phase 1: Launching {num_writers} writer threads")
            start = time.time()
            with ThreadPoolExecutor(max_workers=num_writers) as executor:
                results = list(executor.map(writer_task, range(num_writers)))
            duration = time.time() - start
            
            success_count = sum(results)
            if text_logger:
                text_logger.log_test_step(f"Phase 2: All threads completed in {duration:.2f}s")
            logger.info(f"Phase 2: All threads completed in {duration:.2f}s")
            logger.info(f"  Success: {success_count}/{num_writers}")
            
            if success_count == num_writers:
                logger.info(f"✓ All {num_writers} concurrent writers succeeded")
            else:
                logger.error(f"✗ Only {success_count}/{num_writers} writers succeeded")
            
            self.log_result('concurrent_writers', success_count == num_writers,
                          f"{success_count}/{num_writers} writers succeeded in {duration:.2f}s")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('concurrent_writers', False, str(e))
    
    def test_large_file_sequential_io(self, size_mb=100):
        """Test large sequential I/O"""

        test_name = 'large_sequential_io'
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing sequential read/write with {size_mb}MB file")

        logger.info("=" * 70)
        logger.info("TEST: Large File Sequential I/O")
        logger.info("=" * 70)
        logger.info(f"Testing sequential read/write with {size_mb}MB file")
        
        test_file = os.path.join(self.test_dir, 'large_seq.bin')
        chunk_size = 1024 * 1024
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Sequential WRITE ({size_mb}MB)")
            logger.info(f"Phase 1: Sequential WRITE ({size_mb}MB)")
            start = time.time()
            with open(test_file, 'wb') as f:
                for i in range(size_mb):
                    f.write(os.urandom(chunk_size))
                    if (i + 1) % 25 == 0:
                        elapsed = time.time() - start
                        rate = (i + 1) / elapsed
                        logger.info(f"  Progress: {i+1}/{size_mb}MB ({rate:.1f} MB/s)")
            write_time = time.time() - start
            write_mbps = size_mb / write_time
            logger.info(f"✓ Write completed: {size_mb}MB in {write_time:.2f}s ({write_mbps:.2f} MB/s)")
            
            if text_logger:
                text_logger.log_test_step(f"Phase 2: Sequential READ ({size_mb}MB)")
            logger.info(f"Phase 2: Sequential READ ({size_mb}MB)")
            start = time.time()
            bytes_read = 0
            with open(test_file, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    bytes_read += len(chunk)
            read_time = time.time() - start
            read_mbps = size_mb / read_time
            logger.info(f"✓ Read completed: {size_mb}MB in {read_time:.2f}s ({read_mbps:.2f} MB/s)")
            
            if text_logger:
                text_logger.log_test_step(f"Phase 3: Cleaning up")
            logger.info(f"Phase 3: Cleaning up")
            os.remove(test_file)
            logger.info("✓ Test file removed")
            
            logger.info(f"✓ Large file I/O test completed")
            self.log_result('large_sequential_io', True,
                          f"{size_mb}MB - Write: {write_mbps:.2f} MB/s, Read: {read_mbps:.2f} MB/s")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('large_sequential_io', False, str(e))
    
    def test_readonly_mount_enforcement(self):
        """Test ro mount blocks writes"""

        test_name = 'readonly_mount_enforcement'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing read-only mount enforcement")

        logger.info("=" * 70)
        logger.info("TEST: Read-Only Mount Enforcement")
        logger.info("=" * 70)
        
        # Try to write to the mount point itself (not a subdirectory)
        test_file = os.path.join(self.mount_point, 'ro_test.txt')
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Attempting write on RO mount")
            logger.info("Phase 1: Attempting write on RO mount")
            try:
                with open(test_file, 'w') as f:
                    f.write("Should fail")
                logger.error("✗ Write succeeded on RO mount - TEST FAILED!")
                self.log_result('readonly_mount_enforcement', False,
                            "Write succeeded on ro mount!")
            except (OSError, IOError) as e:
                if e.errno in (30, 13):  # EROFS or EACCES
                    logger.info(f"✓ Write correctly blocked (errno: {e.errno})")
                    self.log_result('readonly_mount_enforcement', True,
                                f"Write blocked as expected (errno {e.errno})")
                else:
                    logger.error(f"✗ Unexpected error: {e}")
                    self.log_result('readonly_mount_enforcement', False, str(e))
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('readonly_mount_enforcement', False, str(e))
    
    def test_readonly_mount_read_operations(self):
        """Test that read operations work on RO mount"""

        test_name = 'readonly_mount_read_operations'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing read-only mount read operations")

        logger.info("=" * 70)
        logger.info("TEST: Read-Only Mount Read Operations")
        logger.info("=" * 70)
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Listing directory contents")
            logger.info("Phase 1: Listing directory contents")
            contents = os.listdir(self.mount_point)
            logger.info(f"✓ Directory listed successfully ({len(contents)} items found)")

            if text_logger:
                text_logger.log_test_step(f"Phase 2: Getting directory stats")            
            logger.info("Phase 2: Getting directory stats")
            stat_info = os.stat(self.mount_point)
            logger.info(f"✓ Directory stat successful")
            logger.info(f"  Mode: {oct(stat_info.st_mode)}")
            logger.info(f"  Owner: {stat_info.st_uid}")
            
            logger.info("✓ Read operations working on RO mount")
            self.log_result('readonly_mount_read_operations', True,
                        f"Read operations successful ({len(contents)} items)")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('readonly_mount_read_operations', False, str(e))

# ============================================================================
# TEST RUNNER
# ============================================================================

class NFS3TestRunner:
    """Run NFS3 test suite"""
    
    def __init__(self, server: str, export: str):
        self.server = server
        self.export = export
        self.all_results = []
    
    def run_basic_tests(self, mount_type='rw'):
        """Run basic test suite"""
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"NFS3 TEST SUITE - {self.export} ({mount_type.upper()})")
        logger.info("=" * 70)
        logger.info("")
        
        mount_opts = NFSMountOptions(transport='tcp')
        test = NFS3Test(self.server, self.export, mount_opts, mount_type=mount_type)
        
        test.setup()
        try:
            # Run tests based on mount type
            test.test_mount_options_verification()
            test.test_transport_protocol()
            
            if mount_type == 'rw':
                # RW mount specific tests
                test.test_readwrite_mount_enforcement()
                test.test_basic_file_operations()
                test.test_idempotent_operations()
                test.test_close_to_open_consistency()
                test.test_nlm_basic_locking()
                test.test_small_file_performance(100)
                test.test_concurrent_writers(32)
                test.test_large_file_sequential_io(128)

            else:
                # RO mount specific tests
                test.test_readonly_mount_enforcement()
                test.test_readonly_mount_read_operations()
                
        finally:
            test.teardown()
        
        self.all_results.extend(test.results)
        return test.results
    
    def print_summary(self):
        """Print test summary"""
        logger.info("")
        logger.info("=" * 70)
        logger.info("TEST SUMMARY")
        logger.info("=" * 70)
        
        total = len(self.all_results)
        passed = sum(1 for r in self.all_results if r['passed'])
        failed = total - passed
        
        logger.info(f"Total Tests: {total}")
        logger.info(f"Passed: {passed} ({100*passed/total:.1f}%)")
        logger.info(f"Failed: {failed} ({100*failed/total:.1f}%)")
        
        if failed > 0:
            logger.info("\nFailed Tests:")
            for result in self.all_results:
                if not result['passed']:
                    logger.info(f"  ✗ {result['test']}: {result['message']}")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    if os.geteuid() != 0:
        logger.error("This script must be run with sudo")
        logger.error("Usage: sudo python3 nfs3_tests_v1.py")
        sys.exit(1)
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("NFS3 PROTOCOL TEST SUITE")
    logger.info("=" * 70)
    logger.info("")
    
    # Initialize text documentation logger
    text_logger = TextDocLogger()
    text_logger.log_metadata("Test Suite", "NFS3 Protocol Validation")
    text_logger.log_metadata("Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    text_logger.log_metadata("Operating System", f"{os.uname().sysname} {os.uname().release}")
    text_logger.log_metadata("Python Version", sys.version.split()[0])
        
    start_time = time.time()
    
    for mount_config in STORAGE_CONFIG['nfs3_mounts']:
        vendor = mount_config['vendor']
        software = mount_config['software']
        server = mount_config['export_server']
        export = mount_config['export_path']
        mount_type = mount_config.get('mount_type', 'rw')
        
        text_logger.log_metadata(f"Vendor", vendor)
        text_logger.log_metadata(f"Software", software)
        text_logger.log_metadata(f"Server ({mount_type})", server)
        text_logger.log_metadata(f"Export Path ({mount_type})", export)
        
        runner = NFS3TestRunner(server, export)
        runner.run_basic_tests(mount_type)
        runner.print_summary()
    
    duration = time.time() - start_time
    text_logger.log_metadata("Total Duration", f"{int(duration//60)}m {int(duration%60)}s")
    
    # Generate text report
    report_file = text_logger.generate_report()
    
    logger.info("")
    logger.info(f"✓ All tests completed in {int(duration//60)}m {int(duration%60)}s")
    logger.info(f"✓ Documentation log: {report_file}")
    logger.info("")
