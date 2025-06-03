import datetime as dt
import json
import os
import random
import requests
import yaml
import re

from dotenv import load_dotenv
from github import Github, GithubException, InputGitTreeElement
import base64
import pandas as pd
import io
import subprocess
from pathlib import Path
from uuid import uuid4
import time
import yaml
import logging
from utils.logger import log_config

# Import our new job system
from job_system.manager import JobManager
from job_system.base import JobStatus

# Import constants
from constants import (
    STATE_PENDING, STATE_RUNNING, STATE_COMPLETED, STATE_FAILED, STATE_EXTERNAL_FAILED,
    STATUS_DICT, JOB_STATUS_MAPPING
)

log = logging.getLogger(__name__)
DEFAULT_LOGGING_FILENAME = 'server_side_run.log'
log_config(level=logging.INFO, filename=DEFAULT_LOGGING_FILENAME, std=True, std_level=logging.INFO)


class FlowManager:
    def __init__(self, gh_token, gh_repo, gh_branch, config_path):
        self.g = Github(gh_token)
        self.repo = self.g.get_repo(gh_repo)
        self.branch = gh_branch
        
        # Initialize the new job manager
        self.job_manager = JobManager(config_path)
        
        # Get configuration for backward compatibility
        config = self.job_manager.get_config()
        
        # Read machine name from config
        self.machine_name = config.get('machine', {}).get('name')
        if not self.machine_name:
            raise ValueError("machine.name not found in config.yaml")
        
        self.scenario_log_path = 'report/scenario.csv'
        self.machine_log_path = f'report/server/{self.machine_name}/log.csv'
        
        self.oneflux_path = config['paths']['oneflux_path']
        self.command = config['paths']['command']
        self.matlab_path = config['paths']['matlab_path']

    def _get_log(self, log_path, csv_header, create_new_log_str, get_log_str):
        try:
            contents = self.repo.get_contents(log_path,
                                              ref=self.branch)
            file_sha = contents.sha
            content = contents.content
            status_str = get_log_str
            data = base64.b64decode(content).decode('utf-8')
            df = pd.read_csv(io.StringIO(data))
        except:
            updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            status_str = create_new_log_str.format(updated_time=updated_time)
            file_status = self.repo.create_file(log_path,
                                                status_str,
                                                csv_header,
                                                branch=self.branch)
            time.sleep(1)
            contents = self.repo.get_contents(log_path,
                                             ref=self.branch)
            file_sha = contents.sha
            content = contents.content
            data = base64.b64decode(content).decode('utf-8')
        df = pd.read_csv(io.StringIO(data))
        return df, status_str
       
    def get_scenario_log(self):
        csv_header = 'uuid,actor,last_updated_timestamp,updated_by,file_path\n'
        get_log_str = 'Get scenario.csv'
        create_new_log_str = 'Create initial scenario.csv at {updated_time}'
        scenario_df, status_str = self._get_log(self.scenario_log_path,
                                                csv_header,
                                                create_new_log_str,
                                                get_log_str)
        return scenario_df, status_str

    def get_machine_log(self):
        csv_header = ('uuid,index,actor,file_path,last_updated_timestamp,'
                      'state,process_id,additional_info,run_uuid,site_id,data_dir\n')
        get_log_str = f'Get log.csv of machine: {self.machine_name}'
        create_new_log_str = f'Create initial log.csv for {self.machine_name} ' + 'at ' + '{updated_time}'
        machine_log_df, status_str = self._get_log(self.machine_log_path,
                                                   csv_header,
                                                   create_new_log_str,
                                                   get_log_str)
        return machine_log_df, status_str

    def update_machine_log(self, uuid, params_index, actor,
                           next_state, process_id, additional_info,
                           run_uuid=None,
                           site_id=None,
                           data_dir=None,
                           file_path=None):
        machine_log_df, _ = self.get_machine_log()
        updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_log = {
            'uuid': [uuid],
            'index': [params_index],
            'actor': [actor],
            'last_updated_timestamp': [updated_time],
            'state': [next_state],
            'process_id': [process_id],
            'additional_info': [additional_info],
            'run_uuid': [run_uuid],
            'site_id': [site_id],
            'data_dir': [data_dir],
            'file_path': [file_path]
        }
        machine_log_df = pd.concat([pd.DataFrame(new_log), machine_log_df])
        updated_content = machine_log_df.to_csv(index=False)
        file_sha = self.repo.get_contents(self.machine_log_path, ref=self.branch).sha
        status_str = f'switch to {additional_info} for run uuid {uuid}-{params_index} at {self.machine_name}, process ID: {process_id}'
        file_status = self.repo.update_file(self.machine_log_path,
                                            status_str,
                                            updated_content,
                                            file_sha,
                                            branch=self.branch)
        time.sleep(1)
        return machine_log_df, status_str

    def run_step_0(self, scenario_log_df, machine_log_df):
        current_state = STATE_PENDING
        new_uuids = (set(scenario_log_df.uuid.tolist()) - set(machine_log_df.uuid.tolist()))
        status_str = 'No new scenario found in scenario log'
        
        for uuid in new_uuids:
            data = scenario_log_df[scenario_log_df['uuid'] == uuid]
            file_path = data['file_path'].values[0]
            actor = data['actor'].values[0]
            scenario_contents = self.repo.get_contents(file_path, ref=self.branch)
            scenario_data = scenario_contents.decoded_content.decode('utf-8')
            scenario_data = yaml.safe_load(scenario_data)
            scenario = scenario_data[0]
            new_machine_log = {}
            new_run_count = {}
            
            if scenario.get('machine') == self.machine_name:
                new_run_count.setdefault(uuid, 0)
                new_run_count[uuid] += 1
                new_machine_runs = scenario.get('scenarios')
                
                for run_index, run_data in enumerate(new_machine_runs):
                    updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    additional_info = STATUS_DICT[STATE_PENDING]
                    new_machine_log.setdefault('uuid', []).append(uuid)
                    new_machine_log.setdefault('index', []).append(run_index)
                    new_machine_log.setdefault('actor', []).append(actor)
                    new_machine_log.setdefault('file_path', []).append(file_path)
                    new_machine_log.setdefault('last_updated_timestamp', []).append(updated_time)
                    new_machine_log.setdefault('state', []).append(current_state)
                    new_machine_log.setdefault('additional_info', []).append(additional_info)
                    new_machine_log.setdefault('run_uuid', []).append(None)
                    new_machine_log.setdefault('site_id', []).append(run_data['params']['siteid'])
                    new_machine_log.setdefault('data_dir', []).append(run_data['params']['datadir'])
                    
                machine_log_df = pd.concat([pd.DataFrame(new_machine_log), machine_log_df])
                
        if new_uuids:
            status_str = ''
            for uuid, count in new_run_count.items():
                status_str += f'Add new {count} runs found from scenario uuid: {uuid} to machine: {self.machine_name}\n'
            updated_content = machine_log_df.to_csv(index=False)
            file_sha = self.repo.get_contents(self.machine_log_path, ref=self.branch).sha
            file_status = self.repo.update_file(self.machine_log_path,
                                                status_str,
                                                updated_content,
                                                file_sha,
                                                branch=self.branch)
            status_str.strip('\n')
        return machine_log_df, status_str

    def is_machine_available(self):
        """Check if machine is available using JobManager"""
        return self.job_manager.is_scheduler_available()

    def run_step_04_1(self, run_state_df):
        """Start pending jobs using the new JobManager"""
        next_state = STATE_RUNNING
        additional_info = STATUS_DICT[STATE_RUNNING]
        status = []
        
        df = run_state_df[run_state_df['state'].isin([STATE_PENDING, STATE_EXTERNAL_FAILED])]
        machine_log_df, _ = self.get_machine_log()
        
        for _, row in df.iterrows():
            if self.is_machine_available():
                job_id, run_uuid, success_message = self.start_process(row)
                if job_id:
                    machine_log_df, s_str = self.update_machine_log(row['uuid'],
                                                                    row['index'],
                                                                    row['actor'],
                                                                    next_state,
                                                                    job_id,
                                                                    additional_info,
                                                                    run_uuid,
                                                                    row['site_id'],
                                                                    row['data_dir'],
                                                                    row['file_path'])
                    status.append(s_str)
                else:
                    log.error(f"Failed to start process: {success_message}")
                    
        status_str = '\n'.join(status)
        return machine_log_df, status_str

    def run_step_1_234(self, run_state_df):
        """Check job status and handle completed jobs"""
        # Create a copy of the DataFrame to avoid SettingWithCopyWarning
        df = run_state_df.copy()
        
        # Convert 'last_updated_timestamp' to datetime if it's not already
        df['last_updated_timestamp'] = pd.to_datetime(df['last_updated_timestamp'])
        
        # Sort the dataframe by uuid, index, and last_updated_timestamp
        df = df.sort_values(['uuid', 'index', 'last_updated_timestamp'], 
                            ascending=[True, True, True])
        
        # Keep only the last row for each uuid and index combination
        df = df.groupby(['uuid', 'index']).last().reset_index()
        
        # Filter rows where state is 1 (running)
        df = df[df['state'] == STATE_RUNNING]
    
        for index, row in df.iterrows():
            job_id = str(int(row['process_id']))
            
            # Check job status using JobManager
            job_status = self.job_manager.check_job_status(job_id)
            
            # Convert JobStatus to old numeric status
            numeric_status = JOB_STATUS_MAPPING.get(job_status, STATE_EXTERNAL_FAILED)
            
            # Handle completed jobs
            if job_status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMEOUT]:
                if job_status == JobStatus.COMPLETED:
                    is_upload_successful, result_path = self.upload_run_result(row['site_id'],
                                                                               row['data_dir'],
                                                                               row['run_uuid'],
                                                                               job_id)
                    if is_upload_successful:
                        self.update_machine_log(row['uuid'],
                                                row['index'], 
                                                row['actor'],
                                                STATE_COMPLETED,  # Completed state
                                                row['process_id'],
                                                result_path,
                                                row['run_uuid'],
                                                row['site_id'],
                                                row['data_dir'],
                                                row['file_path'])
                    else:
                        log.error(f"Job completed but failed to upload results for {row['run_uuid']}")
                elif job_status == JobStatus.FAILED:
                    self.update_machine_log(row['uuid'],
                                            row['index'], 
                                            row['actor'],
                                            STATE_FAILED,  # Failed state
                                            row['process_id'],
                                            STATUS_DICT[STATE_FAILED],
                                            row['run_uuid'],
                                            row['site_id'],
                                            row['data_dir'],
                                            row['file_path'])
                elif job_status in [JobStatus.CANCELLED, JobStatus.TIMEOUT]:
                    # add failed state because of external error/ will need to rerun
                    self.update_machine_log(row['uuid'],
                                            row['index'], 
                                            row['actor'],
                                            STATE_EXTERNAL_FAILED,  # External failure state
                                            row['process_id'],
                                            STATUS_DICT[STATE_EXTERNAL_FAILED],
                                            row['run_uuid'],
                                            row['site_id'],
                                            row['data_dir'],
                                            row['file_path'])
        return '', None

    def upload_run_result(self, site_id, data_dir, run_uuid, process_id):
        """Upload job results using JobManager to get output files"""
        try:
            # Upload original log file
            content_file = Path(self.oneflux_path)/f'{run_uuid}.log'
            with open(content_file, 'r') as f:
                content = f.read()
            file_status = self.repo.create_file(f'report/{site_id}/{run_uuid}/REPORT.log',
                                                f'generate report {run_uuid}',
                                                content, branch=self.branch)
            
            # Get job output files from JobManager
            output_files = self.job_manager.get_job_output_files(process_id)
            
            # Prepare for uploading both log files and image files
            element_list = list()
            master_ref = self.repo.get_git_ref(f'heads/{self.branch}')
            master_sha = master_ref.object.sha
            base_tree = self.repo.get_git_tree(master_sha)
            commit_message = f'Upload results for job {run_uuid}'
            
            # Upload job output files
            for output_file in output_files:
                file_path = Path(output_file)
                if file_path.exists():
                    file_name = file_path.name
                    with open(file_path, 'r') as f:
                        data = f.read()
                    
                    # Create blob and add to element list for commit
                    blob = self.repo.create_git_blob(data, 'base64')
                    element = InputGitTreeElement(
                        path=f'report/{site_id}/{run_uuid}/{file_name}',
                        mode='100644',
                        type='blob',
                        sha=blob.sha
                    )
                    element_list.append(element)
            
            # Continue with uploading image files
            output_img_path = Path(self.oneflux_path)/'data'/data_dir/'99_fluxnet2015'
            png_files = list(output_img_path.glob('*.png'))
            
            for entry in png_files:
                path_in_repo = Path(entry).name
                entry = str(entry)
                with open(entry, 'rb') as input_file:
                    data = input_file.read()
                if entry.endswith('.png'):
                    data = base64.b64encode(data).decode('utf-8')
                blob = self.repo.create_git_blob(data, 'base64')
                element = InputGitTreeElement(
                    path=f'report/{site_id}/{run_uuid}/{path_in_repo}',
                    mode='100644',
                    type='blob',
                    sha=blob.sha
                )
                element_list.append(element)
            
            # Commit all files together
            tree = self.repo.create_git_tree(element_list, base_tree)
            parent = self.repo.get_git_commit(master_sha)
            commit = self.repo.create_git_commit(commit_message, tree, [parent])
            master_ref.edit(commit.sha)
            
            updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            additional_info = f'report/{site_id}/REPORT_{run_uuid}.log'
            
            return True, additional_info
        except Exception as e:
            log.error(f"Error uploading results: {e}")
            return False, None

    def get_run_state(self, machine_log_df):
        """Get current run state (unchanged from original)"""
        try:
            # Convert 'last_updated_timestamp' to datetime if it's not already
            machine_log_df['last_updated_timestamp'] = pd.to_datetime(machine_log_df['last_updated_timestamp'])
            
            # Count occurrences of state 4 for each (uuid, index) combination
            state_4_count = machine_log_df[machine_log_df['state'] == STATE_EXTERNAL_FAILED].groupby(['uuid', 'index']).size().reset_index(name='count')
            
            # Sort the dataframe by uuid, index, and last_updated_timestamp
            machine_log_df = machine_log_df.sort_values(['uuid', 'index', 'last_updated_timestamp'])
            
            # Keep only the last row for each uuid and index combination
            run_state_df = machine_log_df.groupby(['uuid', 'index']).last().reset_index()
            
            # Merge the count of state 4 occurrences into the result
            run_state_df = run_state_df.merge(state_4_count, on=['uuid', 'index'], how='left')
            
            # Fill NaN values in count column with 0 (for rows that never had state 4)
            run_state_df['count'] = run_state_df['count'].fillna(0).astype(int)
            
        except TypeError:
            run_state_df = machine_log_df.copy()
            run_state_df['count'] = 0  # Add count column with default value 0

        # Filter rows based on the new conditions
        filtered_run_state_df = run_state_df[
            (run_state_df['state'] < STATE_COMPLETED) | 
            ((run_state_df['state'] == STATE_EXTERNAL_FAILED) & (run_state_df['count'] < 3))
        ].reset_index(drop=True)

        # Remove rows with state 3
        filtered_run_state_df = filtered_run_state_df[filtered_run_state_df['state'] != STATE_FAILED]

        # Count the occurrences of each state
        state_counts = filtered_run_state_df['state'].value_counts()

        # Create the status string
        if not state_counts.empty:
            status_str = 'There are ' + ', '.join([f"{count} {STATUS_DICT.get(status)}" 
                                                   for status, count in state_counts.items()])
        else:
            status_str = 'No run is running or pending'

        return filtered_run_state_df, status_str

    def start_process(self, data):
        """Start a process using the new JobManager"""
        run_uuid = str(uuid4())
        uuid = data['uuid']
        file_path = data['file_path']
        param_index = data['index']
        
        # Get run parameters from GitHub
        run_file_contents = self.repo.get_contents(file_path, ref=self.branch)
        run_file_data = run_file_contents.decoded_content.decode('utf-8')
        run_file_yaml = yaml.safe_load(run_file_data)
        runs = run_file_yaml[0].get('scenarios')
        run_data = runs[param_index]
        params = run_data.get('params')
        
        # Extract parameters
        workflow_params = {
            'siteid': params.get('siteid'),
            'datadir': params.get('datadir'),
            'firstyear': params.get('firstyear'),
            'lastyear': params.get('lastyear'),
            'custom_params': run_data.get('custom_params', {})
        }
        
        # Determine workflow type from the scenario data
        # For now, default to oneflux, but this could be configurable in the scenario
        workflow_type = run_data.get('workflow_type', 'oneflux')
        
        # Submit job using JobManager
        job_id, success, message = self.job_manager.submit_workflow(
            workflow_type=workflow_type,
            params=workflow_params,
            run_uuid=run_uuid
        )
        
        if success:
            return job_id, run_uuid, message
        else:
            return None, None, message


if __name__ == '__main__':
    load_dotenv()
    
    # Get GitHub configuration from environment variables
    gh_token = os.environ.get('TOKEN')
    gh_repo = os.environ.get('REPO')
    gh_branch = os.environ.get('BRANCH')
    
    # Validate required environment variables
    if not all([gh_token, gh_repo, gh_branch]):
        missing_vars = []
        if not gh_token:
            missing_vars.append('TOKEN')
        if not gh_repo:
            missing_vars.append('REPO')
        if not gh_branch:
            missing_vars.append('BRANCH')
        
        log.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        log.error("Please ensure these are set in your .env file:")
        log.error("  TOKEN=your_github_token")
        log.error("  REPO=username/repository")
        log.error("  BRANCH=report")
        exit(1)
    
    # Read job system configuration
    config_path = 'config.yaml'
    if not Path(config_path).exists():
        log.error(f"Configuration file not found: {config_path}")
        log.error("Please ensure config.yaml exists with proper job system configuration")
        exit(1)
    
    try:
        # Initialize with new configuration
        flow_manager = FlowManager(gh_token, gh_repo, gh_branch, config_path)
        log.info(f"FlowManager initialized successfully for machine: {flow_manager.machine_name}")
        log.info(f"Using repository: {gh_repo}, branch: {gh_branch}")
        
    except Exception as e:
        log.error(f"Failed to initialize FlowManager: {e}")
        exit(1)
    
    # Rest of the execution flow remains the same
    try:
        scenario_log_df, status_str = flow_manager.get_scenario_log()
        log.info(status_str)
        machine_log_df, status_str = flow_manager.get_machine_log()
        log.info(status_str)

        # Update machine log with step 0
        machine_log_df, status_str = flow_manager.run_step_0(scenario_log_df, machine_log_df)
        log.info(status_str)
        
        # Consolidate df
        filtered_run_state_df, status_str = flow_manager.get_run_state(machine_log_df)
        log.info(status_str)

        # Run step 04 -> 1
        machine_log_df, status_str = flow_manager.run_step_04_1(filtered_run_state_df)
        log.info(status_str)

        machine_log_df, status_str = flow_manager.run_step_1_234(machine_log_df)
        log.info(status_str)
        
    except Exception as e:
        log.error(f"Error during execution: {e}")
        exit(1) 