"""
Concrete implementations of workflow types.
"""

from typing import Dict, Any, Tuple, List
from pathlib import Path
import os

from .base import WorkflowType


class OneFluxWorkflow(WorkflowType):
    """OneFlux pipeline workflow implementation"""
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate OneFlux parameters"""
        errors = []
        required_params = self.config.get('required_params', [])
        
        # Check required parameters
        for param in required_params:
            if param not in params:
                errors.append(f"Missing required parameter: {param}")
        
        # Validate specific OneFlux parameters
        if 'firstyear' in params and 'lastyear' in params:
            try:
                first_year = int(params['firstyear'])
                last_year = int(params['lastyear'])
                if first_year > last_year:
                    errors.append("firstyear must be <= lastyear")
                if first_year < 1900 or last_year > 2100:
                    errors.append("Year values must be reasonable (1900-2100)")
            except (ValueError, TypeError):
                errors.append("firstyear and lastyear must be integers")
        
        # Validate siteid format (basic check)
        if 'siteid' in params:
            siteid = params['siteid']
            if not isinstance(siteid, str) or len(siteid) < 3:
                errors.append("siteid must be a string with at least 3 characters")
        
        return len(errors) == 0, errors
    
    def build_command(self, params: Dict[str, Any], paths: Dict[str, str]) -> str:
        """Build OneFlux execution command"""
        # Prepare custom parameters string
        custom_params_str = ""
        custom_params = params.get('custom_params', {})
        if custom_params:
            for key, value in custom_params.items():
                custom_params_str += f"--{key} {value} "
        
        # Build the command using the template
        command_template = self.config.get('command_template')
        
        # Prepare the format dictionary
        format_dict = {
            'command': paths.get('command'),
            'data_path': Path(paths.get('oneflux_path', '')) / 'data',
            'siteid': params.get('siteid'),
            'datadir': params.get('datadir'),
            'firstyear': params.get('firstyear'),
            'lastyear': params.get('lastyear'),
            'log_path': Path(paths.get('oneflux_path', '')) / f"{params.get('run_uuid', 'unknown')}.log",
            'matlab_path': paths.get('matlab_path'),
            'custom_params': custom_params_str.strip()
        }
        
        return command_template.format(**format_dict)
    
    def get_environment_setup(self) -> List[str]:
        """Get OneFlux environment setup commands"""
        return self.config.get('environment_setup', [])
    
    def get_job_name(self, params: Dict[str, Any], run_uuid: str) -> str:
        """Generate OneFlux-specific job name"""
        siteid = params.get('siteid', 'unknown')
        return f"oneflux_{siteid}_{run_uuid[:8]}"


class GenericPythonWorkflow(WorkflowType):
    """Generic Python script workflow implementation"""
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate generic Python workflow parameters"""
        errors = []
        required_params = self.config.get('required_params', [])
        
        # Check required parameters
        for param in required_params:
            if param not in params:
                errors.append(f"Missing required parameter: {param}")
        
        # Validate script parameter exists and is a valid path
        if 'script' in params:
            script_path = Path(params['script'])
            if not script_path.exists():
                errors.append(f"Script file does not exist: {script_path}")
            elif not script_path.is_file():
                errors.append(f"Script path is not a file: {script_path}")
        
        return len(errors) == 0, errors
    
    def build_command(self, params: Dict[str, Any], paths: Dict[str, str]) -> str:
        """Build generic Python execution command"""
        command_template = self.config.get('command_template')
        
        format_dict = {
            'script': params.get('script'),
            'args': params.get('args', '')
        }
        
        return command_template.format(**format_dict)
    
    def get_environment_setup(self) -> List[str]:
        """Get generic Python environment setup commands"""
        return self.config.get('environment_setup', [])


class CustomScriptWorkflow(WorkflowType):
    """Custom shell script workflow implementation"""
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate custom script workflow parameters"""
        errors = []
        required_params = self.config.get('required_params', [])
        
        # Check required parameters
        for param in required_params:
            if param not in params:
                errors.append(f"Missing required parameter: {param}")
        
        # Validate script_path parameter exists and is executable
        if 'script_path' in params:
            script_path = Path(params['script_path'])
            if not script_path.exists():
                errors.append(f"Script file does not exist: {script_path}")
            elif not script_path.is_file():
                errors.append(f"Script path is not a file: {script_path}")
            elif not os.access(script_path, os.X_OK):
                errors.append(f"Script is not executable: {script_path}")
        
        return len(errors) == 0, errors
    
    def build_command(self, params: Dict[str, Any], paths: Dict[str, str]) -> str:
        """Build custom script execution command"""
        command_template = self.config.get('command_template')
        
        format_dict = {
            'script_path': params.get('script_path'),
            'args': params.get('args', '')
        }
        
        return command_template.format(**format_dict)
    
    def get_environment_setup(self) -> List[str]:
        """Get custom script environment setup commands"""
        return self.config.get('environment_setup', []) 