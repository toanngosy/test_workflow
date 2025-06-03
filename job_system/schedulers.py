"""
Concrete implementations of job schedulers.
"""

import subprocess
import re
import os
import signal
import time
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

from .base import JobScheduler, JobStatus


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
        self.active_jobs = {}  # job_id -> subprocess.Popen
    
    def submit_job(self, job_script_path: str) -> Tuple[Optional[str], bool]:
        """Execute job locally using subprocess"""
        try:
            if len(self.active_jobs) >= self.max_concurrent:
                return None, False
                
            # Generate a simple job ID (timestamp + PID)
            job_id = f"local_{int(time.time())}"
            
            # Start the process
            process = subprocess.Popen(
                ['bash', str(job_script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_dir
            )
            
            self.active_jobs[job_id] = process
            return job_id, True
            
        except subprocess.SubprocessError:
            return None, False
    
    def check_job_status(self, job_id: str) -> JobStatus:
        """Check local job status"""
        if job_id not in self.active_jobs:
            return JobStatus.UNKNOWN
            
        process = self.active_jobs[job_id]
        poll_result = process.poll()
        
        if poll_result is None:
            return JobStatus.RUNNING
        elif poll_result == 0:
            # Clean up completed job
            del self.active_jobs[job_id]
            return JobStatus.COMPLETED
        else:
            # Clean up failed job
            del self.active_jobs[job_id]
            return JobStatus.FAILED
    
    def is_available(self) -> bool:
        """Check if we can run more local jobs"""
        # Clean up any completed jobs
        completed_jobs = []
        for job_id, process in self.active_jobs.items():
            if process.poll() is not None:
                completed_jobs.append(job_id)
        
        for job_id in completed_jobs:
            del self.active_jobs[job_id]
            
        return len(self.active_jobs) < self.max_concurrent
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a local job"""
        if job_id not in self.active_jobs:
            return False
            
        try:
            process = self.active_jobs[job_id]
            process.terminate()
            
            # Wait a bit for graceful termination
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if it doesn't terminate gracefully
                process.kill()
                process.wait()
                
            del self.active_jobs[job_id]
            return True
            
        except (ProcessLookupError, subprocess.SubprocessError):
            return False
    
    def get_job_output_files(self, job_id: str) -> List[str]:
        """Get local job output files"""
        # For local jobs, we could write output to files if needed
        # For now, return empty list as output is captured by subprocess
        return []


class PBSScheduler(JobScheduler):
    """PBS/Torque scheduler implementation (basic structure)"""
    
    def submit_job(self, job_script_path: str) -> Tuple[Optional[str], bool]:
        """Submit job to PBS using qsub"""
        try:
            cmd = ['qsub', str(job_script_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # PBS returns job ID directly
                job_id = result.stdout.strip()
                return job_id, True
                
            return None, False
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            return None, False
    
    def check_job_status(self, job_id: str) -> JobStatus:
        """Check PBS job status using qstat"""
        try:
            cmd = ['qstat', '-f', str(job_id)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                # Parse PBS status from qstat output
                # This is a simplified implementation
                if 'job_state = R' in result.stdout:
                    return JobStatus.RUNNING
                elif 'job_state = Q' in result.stdout:
                    return JobStatus.PENDING
                elif 'job_state = C' in result.stdout:
                    return JobStatus.COMPLETED
                    
            return JobStatus.UNKNOWN
            
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            return JobStatus.UNKNOWN
    
    def is_available(self) -> bool:
        """Check if PBS is available"""
        try:
            result = subprocess.run(['qstat', '--version'], capture_output=True, timeout=5)
            return result.returncode == 0
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            return False
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel PBS job using qdel"""
        try:
            cmd = ['qdel', str(job_id)]
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            return result.returncode == 0
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            return False
    
    def get_job_output_files(self, job_id: str) -> List[str]:
        """Get PBS job output files"""
        # PBS typically creates .o<jobid> and .e<jobid> files
        output_files = []
        base_name = job_id.split('.')[0]  # Remove server part if present
        
        out_file = Path(f"{base_name}.o{job_id}")
        err_file = Path(f"{base_name}.e{job_id}")
        
        if out_file.exists():
            output_files.append(str(out_file))
        if err_file.exists():
            output_files.append(str(err_file))
            
        return output_files 