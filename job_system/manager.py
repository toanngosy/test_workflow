"""
Job Manager that orchestrates workflow execution and job scheduling.
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from uuid import uuid4
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from .base import JobScheduler, WorkflowType, JobStatus
from .schedulers import SlurmScheduler, LocalScheduler, PBSScheduler
from .workflows import OneFluxWorkflow, GenericPythonWorkflow, CustomScriptWorkflow


class JobManager:
    """Central manager for job execution across different schedulers and workflows"""
    
    def __init__(self, config_path: str):
        """
        Initialize JobManager with configuration.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config = self._load_config(config_path)
        self.scheduler = self._create_scheduler()
        self.workflow_types = self._create_workflow_types()
        self.template_engine = self._setup_templates()
        
        # Create necessary directories
        self._ensure_directories()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration file"""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Basic validation
        required_sections = ['scheduler', 'workflow_types', 'paths', 'machine']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        return config
    
    def _create_scheduler(self) -> JobScheduler:
        """Create the appropriate job scheduler instance"""
        scheduler_type = self.config['scheduler']['type']
        scheduler_config = self.config['scheduler'][scheduler_type]
        
        schedulers = {
            'slurm': SlurmScheduler,
            'pbs': PBSScheduler,
            'local': LocalScheduler
        }
        
        if scheduler_type not in schedulers:
            raise ValueError(f"Unsupported scheduler type: {scheduler_type}")
        
        return schedulers[scheduler_type](scheduler_config)
    
    def _create_workflow_types(self) -> Dict[str, WorkflowType]:
        """Create workflow type instances"""
        workflows = {}
        
        workflow_classes = {
            'oneflux': OneFluxWorkflow,
            'generic_python': GenericPythonWorkflow,
            'custom_script': CustomScriptWorkflow
        }
        
        for workflow_name, workflow_config in self.config['workflow_types'].items():
            if workflow_name in workflow_classes:
                workflows[workflow_name] = workflow_classes[workflow_name](workflow_config)
            else:
                raise ValueError(f"Unsupported workflow type: {workflow_name}")
        
        return workflows
    
    def _setup_templates(self) -> Environment:
        """Setup Jinja2 template environment"""
        template_dir = Path(__file__).parent.parent / 'templates'
        if not template_dir.exists():
            template_dir.mkdir(parents=True)
        
        return Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
    def _ensure_directories(self):
        """Create necessary directories if they don't exist"""
        paths = self.config['paths']
        
        # Create script output directory
        script_dir = Path(paths.get('script_output_dir', '/tmp/scripts'))
        script_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log output directory
        log_dir = Path(paths.get('log_output_dir', '/tmp/logs'))
        log_dir.mkdir(parents=True, exist_ok=True)
    
    def submit_workflow(self, workflow_type: str, params: Dict[str, Any], run_uuid: Optional[str] = None) -> Tuple[Optional[str], bool, str]:
        """
        Submit a workflow for execution.
        
        Args:
            workflow_type: Type of workflow to execute
            params: Workflow parameters
            run_uuid: Optional run UUID (will be generated if not provided)
            
        Returns:
            Tuple of (job_id, success_flag, status_message)
        """
        if run_uuid is None:
            run_uuid = str(uuid4())
        
        # Validate workflow type
        if workflow_type not in self.workflow_types:
            return None, False, f"Unknown workflow type: {workflow_type}"
        
        workflow = self.workflow_types[workflow_type]
        
        # Add run_uuid to params for use in workflow
        params = params.copy()
        params['run_uuid'] = run_uuid
        
        # Validate parameters
        is_valid, errors = workflow.validate_params(params)
        if not is_valid:
            error_msg = f"Parameter validation failed: {'; '.join(errors)}"
            return None, False, error_msg
        
        # Check if scheduler is available
        if not self.scheduler.is_available():
            return None, False, "Scheduler is not available"
        
        try:
            # Generate job script
            job_script_path = self._generate_job_script(workflow_type, workflow, params, run_uuid)
            
            # Submit job
            job_id, success = self.scheduler.submit_job(job_script_path)
            
            if success:
                return job_id, True, f"Job submitted successfully with ID: {job_id}"
            else:
                return None, False, "Failed to submit job to scheduler"
                
        except Exception as e:
            return None, False, f"Error submitting job: {str(e)}"
    
    def _generate_job_script(self, workflow_type: str, workflow: WorkflowType, params: Dict[str, Any], run_uuid: str) -> str:
        """Generate job script using templates"""
        # Get scheduler template
        scheduler_config = self.config['scheduler'][self.config['scheduler']['type']]
        template_name = scheduler_config.get('template', 'slurm_job.sh.j2')
        
        try:
            template = self.template_engine.get_template(template_name)
        except TemplateNotFound:
            raise ValueError(f"Template not found: {template_name}")
        
        # Build command
        command = workflow.build_command(params, self.config['paths'])
        
        # Get job name
        job_name = workflow.get_job_name(params, run_uuid)
        
        # Prepare template variables
        template_vars = {
            'job_name': job_name,
            'run_uuid': run_uuid,
            'command': command,
            'environment_setup': workflow.get_environment_setup(),
            'working_dir': self.config['paths'].get('oneflux_path'),  # Default working directory
        }
        
        # Add scheduler-specific parameters
        if self.config['scheduler']['type'] == 'slurm':
            template_vars['slurm_params'] = scheduler_config.get('default_params', {})
        
        # Render template
        script_content = template.render(**template_vars)
        
        # Write script to file
        script_dir = Path(self.config['paths']['script_output_dir'])
        script_path = script_dir / f"job_{run_uuid}.sh"
        
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        script_path.chmod(0o755)
        
        return str(script_path)
    
    def check_job_status(self, job_id: str) -> JobStatus:
        """Check the status of a job"""
        return self.scheduler.check_job_status(job_id)
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        return self.scheduler.cancel_job(job_id)
    
    def get_job_output_files(self, job_id: str) -> list[str]:
        """Get output files for a job"""
        return self.scheduler.get_job_output_files(job_id)
    
    def is_scheduler_available(self) -> bool:
        """Check if the scheduler is available"""
        return self.scheduler.is_available()
    
    def get_supported_workflow_types(self) -> list[str]:
        """Get list of supported workflow types"""
        return list(self.workflow_types.keys())
    
    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration"""
        return self.config.copy()
    
    def validate_workflow_params(self, workflow_type: str, params: Dict[str, Any]) -> Tuple[bool, list[str]]:
        """Validate parameters for a specific workflow type"""
        if workflow_type not in self.workflow_types:
            return False, [f"Unknown workflow type: {workflow_type}"]
        
        workflow = self.workflow_types[workflow_type]
        return workflow.validate_params(params) 