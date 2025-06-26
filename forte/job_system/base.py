"""
Abstract base classes for job scheduling and workflow management.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional, List
from enum import Enum


class JobStatus(Enum):
    """Standardized job status across different schedulers"""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class JobScheduler(ABC):
    """Abstract base class for job schedulers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    @abstractmethod
    def submit_job(self, job_script_path: str) -> Tuple[Optional[str], bool]:
        """
        Submit a job to the scheduler.
        
        Args:
            job_script_path: Path to the job script to execute
            
        Returns:
            Tuple of (job_id, success_flag)
        """
        pass
    
    @abstractmethod
    def check_job_status(self, job_id: str) -> JobStatus:
        """
        Check the status of a submitted job.
        
        Args:
            job_id: The job identifier returned by submit_job
            
        Returns:
            JobStatus enum value
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if the scheduler can accept new jobs.
        
        Returns:
            True if scheduler is available, False otherwise
        """
        pass
    
    @abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running or pending job.
        
        Args:
            job_id: The job identifier to cancel
            
        Returns:
            True if cancellation was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_job_output_files(self, job_id: str) -> List[str]:
        """
        Get the paths to output files for a completed job.
        
        Args:
            job_id: The job identifier
            
        Returns:
            List of file paths containing job output
        """
        pass


class WorkflowType(ABC):
    """Abstract base class for different workflow types"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    @abstractmethod
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate the parameters for this workflow type.
        
        Args:
            params: Dictionary of workflow parameters
            
        Returns:
            Tuple of (is_valid, list_of_error_messages)
        """
        pass
    
    @abstractmethod
    def build_command(self, params: Dict[str, Any], paths: Dict[str, str]) -> str:
        """
        Build the execution command for this workflow.
        
        Args:
            params: Workflow-specific parameters
            paths: System paths from configuration
            
        Returns:
            Command string to execute
        """
        pass
    
    @abstractmethod
    def get_environment_setup(self) -> List[str]:
        """
        Get the environment setup commands for this workflow.
        
        Returns:
            List of shell commands to set up the environment
        """
        pass
    
    def get_job_name(self, params: Dict[str, Any], run_uuid: str) -> str:
        """
        Generate a job name for this workflow.
        
        Args:
            params: Workflow parameters
            run_uuid: Unique run identifier
            
        Returns:
            Job name string
        """
        workflow_name = self.__class__.__name__.replace('Workflow', '').lower()
        return f"{workflow_name}_{run_uuid[:8]}" 