"""
Concrete implementations of job schedulers.
"""

import subprocess
import re
import os
import signal
import time
import threading
import psutil  # Add psutil for better process management
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

try:
    from .base import JobScheduler, JobStatus
except ImportError:
    from forte.job_system.base import JobScheduler, JobStatus


class SlurmScheduler(JobScheduler):
    """SLURM workload manager implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_dir = Path(config.get('output_dir', '/tmp'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def submit_job(self, job_script_path: str) -> Tuple[Optional[str], bool]:
        """Submit job to SLURM using sbatch"""
        try:
            cmd = ['sbatch', str(job_script_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                # Parse job ID from output: "Submitted batch job 12345"
                match = re.search(r'Submitted batch job (\d+)', result.stdout)
                if match:
                    job_id = match.group(1)
                    return job_id, True
                    
            return None, False
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            return None, False
    
    def check_job_status(self, job_id: str) -> JobStatus:
        """Check job status using sacct"""
        try:
            cmd = ['sacct', '-j', str(job_id), '--format=State', '--noheader', '--parsable2']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if lines and lines[0]:
                    slurm_state = lines[0].strip()
                    return self._map_slurm_status(slurm_state)
                    
            return JobStatus.UNKNOWN
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            return JobStatus.UNKNOWN
    
    def is_available(self) -> bool:
        """Check if SLURM can accept new jobs"""
        try:
            # Check if we can run squeue (indicates SLURM is available)
            result = subprocess.run(['squeue', '--version'], capture_output=True, timeout=5)
            if result.returncode != 0:
                return False
                
            # Check number of queued jobs if configured
            max_queued = self.config.get('availability_check', {}).get('params', {}).get('max_queued_jobs')
            if max_queued:
                result = subprocess.run(['squeue', '-u', os.getenv('USER', ''), '-h'], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    queued_jobs = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
                    return queued_jobs < max_queued
                    
            return True
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            return False
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a SLURM job using scancel"""
        try:
            cmd = ['scancel', str(job_id)]
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            return result.returncode == 0
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            return False
    
    def get_job_output_files(self, job_id: str) -> List[str]:
        """Get SLURM job output files"""
        output_files = []
        
        # Standard SLURM output files
        out_file = self.output_dir / f"{job_id}.out"
        err_file = self.output_dir / f"{job_id}.err"
        
        if out_file.exists():
            output_files.append(str(out_file))
        if err_file.exists():
            output_files.append(str(err_file))
            
        return output_files
    
    def _map_slurm_status(self, slurm_state: str) -> JobStatus:
        """Map SLURM job states to our JobStatus enum"""
        state_mapping = {
            'PENDING': JobStatus.PENDING,
            'RUNNING': JobStatus.RUNNING,
            'COMPLETED': JobStatus.COMPLETED,
            'FAILED': JobStatus.FAILED,
            'CANCELLED': JobStatus.CANCELLED,
            'TIMEOUT': JobStatus.TIMEOUT,
            'OUT_OF_MEMORY': JobStatus.FAILED,
            'NODE_FAIL': JobStatus.FAILED,
            'SUSPENDED': JobStatus.PENDING,
        }
        return state_mapping.get(slurm_state, JobStatus.UNKNOWN)


class LocalScheduler(JobScheduler):
    """Local execution scheduler (no actual scheduler)"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.working_dir = Path(config.get('working_directory', '/tmp/local_jobs'))
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = config.get('max_concurrent_jobs', 4)
        # No longer storing subprocess objects - we'll work with PIDs directly
    
    def submit_job(self, job_script_path: str) -> Tuple[Optional[str], bool]:
        """Execute job locally using subprocess"""
        try:
            if not self.is_available():
                print(f"DEBUG: Scheduler not available")
                return None, False
                
            # Create output files first
            temp_job_id = str(int(time.time() * 1000000))  # Temporary ID for file creation
            stdout_file = self.working_dir / f"{temp_job_id}.out"
            stderr_file = self.working_dir / f"{temp_job_id}.err"
            
            print(f"DEBUG: Created temporary files: {stdout_file}, {stderr_file}")
            
            # Open files for direct redirection
            with open(stdout_file, 'w') as stdout_f, open(stderr_file, 'w') as stderr_f:
                # Start the process with direct file redirection
                print(f"DEBUG: Starting process with script: {job_script_path}")
                process = subprocess.Popen(
                    ['bash', str(job_script_path)],
                    stdout=stdout_f,
                    stderr=stderr_f,
                    cwd=self.working_dir,
                    start_new_session=True
                )
            
            # Get the actual PID and rename output files
            job_id = str(process.pid)
            actual_stdout_file = self.working_dir / f"{job_id}.out"
            actual_stderr_file = self.working_dir / f"{job_id}.err"
            
            print(f"DEBUG: Process started with PID: {job_id}")
            
            # Rename files to use actual PID
            stdout_file.rename(actual_stdout_file)
            stderr_file.rename(actual_stderr_file)
            
            print(f"DEBUG: Renamed output files to: {actual_stdout_file}, {actual_stderr_file}")
            
            return job_id, True
            
        except (subprocess.SubprocessError, OSError) as e:
            print(f"DEBUG: Exception in submit_job: {e}")
            return None, False
    
    def check_job_status(self, job_id: str) -> JobStatus:
        """Check local job status using PID"""
        try:
            pid = int(job_id)
            
            # Check if process exists using psutil
            if psutil.pid_exists(pid):
                try:
                    process = psutil.Process(pid)
                    status = process.status()
                    
                    if status == psutil.STATUS_RUNNING:
                        return JobStatus.RUNNING
                    elif status == psutil.STATUS_SLEEPING:
                        return JobStatus.RUNNING  # Still considered running
                    elif status == psutil.STATUS_ZOMBIE:
                        # Process finished, check exit code if available
                        try:
                            exit_code = process.wait(timeout=0.1)  # Non-blocking wait
                            return JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED
                        except psutil.TimeoutExpired:
                            return JobStatus.RUNNING  # Still zombie, treat as running
                    else:
                        # Other statuses (stopped, etc.)
                        return JobStatus.FAILED
                        
                except psutil.NoSuchProcess:
                    # Process disappeared between pid_exists check and Process creation
                    # Check if we have output files to determine if it completed
                    stdout_file = self.working_dir / f"{job_id}.out"
                    stderr_file = self.working_dir / f"{job_id}.err"
                    
                    if stdout_file.exists() or stderr_file.exists():
                        return JobStatus.COMPLETED
                    else:
                        return JobStatus.UNKNOWN
            else:
                # Process doesn't exist - check if we have output files
                stdout_file = self.working_dir / f"{job_id}.out"
                stderr_file = self.working_dir / f"{job_id}.err"
                
                if stdout_file.exists() or stderr_file.exists():
                    return JobStatus.COMPLETED
                else:
                    return JobStatus.UNKNOWN
                
        except (ValueError, psutil.Error):
            return JobStatus.UNKNOWN
    
    def is_available(self) -> bool:
        """Check if we can run more local jobs by counting running processes"""
        try:
            # Count currently running processes started by this scheduler
            # We'll look for bash processes in our working directory
            running_count = 0
            
            for proc in psutil.process_iter(['pid', 'name', 'cwd', 'cmdline']):
                try:
                    # Check if it's a bash process in our working directory
                    if (proc.info['name'] == 'bash' and 
                        proc.info['cwd'] == str(self.working_dir)):
                        running_count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                    
            return running_count < self.max_concurrent
            
        except psutil.Error:
            # If we can't check, assume we're available
            return True
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a local job using PID"""
        try:
            pid = int(job_id)
            
            if psutil.pid_exists(pid):
                process = psutil.Process(pid)
                
                # Try graceful termination first
                process.terminate()
                
                # Wait a bit for graceful termination
                try:
                    process.wait(timeout=5)
                except psutil.TimeoutExpired:
                    # Force kill if it doesn't terminate gracefully
                    process.kill()
                    process.wait()
                
                return True
            else:
                # Process doesn't exist, consider it "cancelled"
                return True
                
        except (ValueError, psutil.NoSuchProcess, psutil.AccessDenied, psutil.Error):
            return False
    
    def get_job_output_files(self, job_id: str) -> List[str]:
        """Get local job output files"""
        output_files = []
        
        # Look for output files named with the job_id (PID)
        stdout_file = self.working_dir / f"{job_id}.out"
        stderr_file = self.working_dir / f"{job_id}.err"
        
        if stdout_file.exists():
            output_files.append(str(stdout_file))
        if stderr_file.exists():
            output_files.append(str(stderr_file))
            
        return output_files


 