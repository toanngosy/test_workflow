import datetime as dt
import json
import os
import random
import requests
import yaml

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
import psutil
import logging
from utils.logger import log_config

log = logging.getLogger(__name__)
DEFAULT_LOGGING_FILENAME = 'server_side_run.log'
log_config(level=logging.INFO, filename=DEFAULT_LOGGING_FILENAME, std=True, std_level=logging.INFO)
status_dict = {
    0: 'pending',
    1: 'running',
    2: 'done',
    3: 'failed',
    4: 'external failed'
}
# oneflux_path = '/home/portnoy/u0/sytoanngo/ONEFlux'
# oneflux_input = '/home/portnoy/u0/sytoanngo/ONEFlux/data/US-ARc_sample_input'
# oneflux_log = 'test_run_name.log'
# script_path = '/home/portnoy/u0/sytoanngo/test_workflow/oneflux.sh'
# run_id = '283a5b57-5266-4cb4-ad84-990c6b69b3e2'
# site_id = 'US-ARc'
server_config_path = '/home/portnoy/u0/sytoanngo/test_workflow/oneflux_run.yaml'
script_path = '/home/portnoy/u0/sytoanngo/test_workflow/oneflux_run_template.sh'

class FlowManager:
    def __init__(self, gh_token, gh_repo, gh_branch, machine_name):
        self.g = Github(gh_token)
        self.repo = self.g.get_repo(gh_repo)
        self.branch = gh_branch
        self.machine_name = machine_name
        self.scenario_log_path = 'report/scenario.csv'
        self.machine_log_path = f'report/server/{self.machine_name}/log.csv'

        with open(server_config_path, 'r') as file:
            data = yaml.safe_load(file)
        self.oneflux_path = data.get('oneflux_path')
        self.command = data.get('command')
        self.matlab_path = data.get('matlab_path')

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
        status_str = f'switch to {additional_info} for run uuid {uuid}-{params_index} at {machine_name}'
        file_status = self.repo.update_file(self.machine_log_path,
                                            status_str,
                                            updated_content,
                                            file_sha,
                                            branch=self.branch)
        time.sleep(1)
        return machine_log_df, status_str

    def run_step_0(self, scenario_log_df, machine_log_df):
        current_state = 0
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
                    additional_info = 'pending'
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
                status_str += f'Add new {count} runs found from scenario uuid: {uuid} to machine: {machine_name}\n'
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
        # TODO: implement
        return True

    def run_step_04_1(self, run_state_df):
        # check if machine is available, start the run
        # add run to the log, change state from 0 -> 1
        # if cannot start the run, don't add
        next_state = 1
        additional_info = 'running'
        status = []
        
        df = run_state_df[run_state_df['state'].isin([0, 4])]
        machine_log_df, _ = self.get_machine_log()
        for _, row in df.iterrows():
            # TODO: do we have the assumption that when machine avail
            # it should be able to run the process? add condition to not do that
            if self.is_machine_available():
                process_pid, run_uuid = self.start_process(row)
                machine_log_df, s_str = self.update_machine_log(row['uuid'],
                                                                row['index'],
                                                                row['actor'],
                                                                next_state,
                                                                process_pid,
                                                                additional_info,
                                                                run_uuid,
                                                                row['site_id'],
                                                                row['data_dir'],
                                                                row['file_path'])
                status.append(s_str)
        status_str = '\n'.join(status)
        return machine_log_df, status_str

    def run_step_1_234(self, run_state_df):
        # 2: succeed
        # 3: code/data fail
        # 4: fail external, run again

        # check if run is finish, add run to the log, change state from 1 -> 2 or 3
        # if not finish, do nothing
        # condition here is to check if the run is not finished or failed externally
        # Create a copy of the DataFrame to avoid SettingWithCopyWarning
        df = run_state_df.copy()
        
        # Convert 'last_updated_timestamp' to datetime if it's not already
        df['last_updated_timestamp'] = pd.to_datetime(df['last_updated_timestamp'])
        
        # Sort the dataframe by uuid, index, and last_updated_timestamp
        df = df.sort_values(['uuid', 'index', 'last_updated_timestamp'], 
                            ascending=[True, True, True])
        
        # Keep only the last row for each uuid and index combination
        # This will be the row with the latest last_updated_timestamp
        df = df.groupby(['uuid', 'index']).last().reset_index()
        
        # Filter rows where state is 1
        df = df[df['state'] == 1]
    
        for index, row in df.iterrows():
            process_pid = int(row['process_id'])
            is_done = False
            next_state = -1
            # TODO: remove this enforcement when run in cron job
            while not is_done:
                try:
                    process = psutil.Process(process_pid)
                    if process.is_running():
                        status = process.status()
                        if status == psutil.STATUS_ZOMBIE:
                            is_done = True
                            pid, status = os.waitpid(process_pid, os.WNOHANG)
                            if os.WIFEXITED(status):
                                exit_code = os.WEXITSTATUS(status)
                                if exit_code == 0:
                                    next_state = 2
                                elif exit_code == 3:
                                    next_state = 3
                                else:
                                    next_state = 4
                        elif status in (psutil.STATUS_RUNNING,
                                        psutil.STATUS_SLEEPING,
                                        psutil.STATUS_DISK_SLEEP):
                            is_done = False
                except psutil.NoSuchProcess:
                    is_done = True
                    next_state = 4
                except Exception as e:
                    return f'An error occurred: {e}'

            # now check if process 2, 3 or 4
            if is_done:
                if next_state == 2:
                    # get the result and upload
                    is_upload_successful, result_path = self.upload_run_result(row['site_id'],
                                                                               row['data_dir'],
                                                                               row['run_uuid'])
                    if is_upload_successful:
                        self.update_machine_log(row['uuid'],
                                                row['index'], 
                                                row['actor'],
                                                next_state,
                                                row['process_id'],
                                                result_path,
                                                row['run_uuid'],
                                                row['site_id'],
                                                row['data_dir'],
                                                row['file_path'])
                    else:
                        # TODO: add error handle here, succeed but can't upload result?
                        pass
                elif next_state == 3:
                    self.update_machine_log(row['uuid'],
                                            row['index'], 
                                            row['actor'],
                                            next_state,
                                            row['process_id'],
                                            'failed code/data',
                                            row['run_uuid'],
                                            row['site_id'],
                                            row['data_dir'],
                                            row['file_path'])
                elif next_state == 4:
                    # add failed state because of external error/ will need to rerun
                    self.update_machine_log(row['uuid'],
                                            row['index'], 
                                            row['actor'],
                                            next_state,
                                            row['process_id'],
                                            'failed',
                                            row['run_uuid'],
                                            row['site_id'],
                                            row['data_dir'],
                                            row['file_path'])
        return '', None

    def upload_run_result(self, site_id, data_dir, run_uuid):
        try:
            content_file = Path(self.oneflux_path)/f'{run_uuid}.log'
            with open(content_file, 'r') as f:
                content = f.read()
            file_status = self.repo.create_file(f'report/{site_id}/{run_uuid}/REPORT.log',
                                                f'generate report {run_uuid}',
                                                content, branch=self.branch)
            updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            additional_info = f'report/{site_id}/REPORT_{run_uuid}.log'
            
            output_img_path = Path(self.oneflux_path)/'data'/data_dir/'99_fluxnet2015'
            png_files = list(output_img_path.glob('*.png'))
            element_list = list()
            master_ref = self.repo.get_git_ref(f'heads/{self.branch}')
            master_sha = master_ref.object.sha
            base_tree = self.repo.get_git_tree(master_sha)
            commit_message = 'test upload images'
            for entry in png_files:
                path_in_repo = Path(entry).name
                entry = str(entry)
                with open(entry, 'rb') as input_file:
                    data = input_file.read()
                if entry.endswith('.png'):
                    data = base64.b64encode(data).decode('utf-8')
                blob = self.repo.create_git_blob(data, 'base64')
                element = InputGitTreeElement(path=f'report/{site_id}/{run_uuid}/{path_in_repo}', mode='100644', type='blob', sha=blob.sha)
                element_list.append(element)
            tree = self.repo.create_git_tree(element_list, base_tree)
            parent = self.repo.get_git_commit(master_sha)
            commit = self.repo.create_git_commit(commit_message, tree, [parent])
            master_ref.edit(commit.sha)
            return True, additional_info
        except:
            return False, None

    def get_run_state(self, machine_log_df):
        try:
            # Convert 'last_updated_timestamp' to datetime if it's not already
            machine_log_df['last_updated_timestamp'] = pd.to_datetime(machine_log_df['last_updated_timestamp'])
            
            # Count occurrences of state 4 for each (uuid, index) combination
            state_4_count = machine_log_df[machine_log_df['state'] == 4].groupby(['uuid', 'index']).size().reset_index(name='count')
            
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
            (run_state_df['state'] < 2) | 
            ((run_state_df['state'] == 4) & (run_state_df['count'] < 3))
        ].reset_index(drop=True)

        # Remove rows with state 3
        filtered_run_state_df = filtered_run_state_df[filtered_run_state_df['state'] != 3]

        # Count the occurrences of each state
        state_counts = filtered_run_state_df['state'].value_counts()

        # Create the status string
        if not state_counts.empty:
            status_str = 'There are ' + ', '.join([f"{count} {status_dict.get(status)}" 
                                                   for status, count in state_counts.items()])
        else:
            status_str = 'No run is running or pending'

        return filtered_run_state_df, status_str

    def start_process(self, data):
        run_uuid = str(uuid4())
        uuid = data['uuid']
        file_path = data['file_path']
        param_index = data['index']
        run_file_contents = self.repo.get_contents(file_path, ref=self.branch)
        run_file_data = run_file_contents.decoded_content.decode('utf-8')
        run_file_yaml = yaml.safe_load(run_file_data)
        runs = run_file_yaml[0].get('scenarios')
        run_data = runs[param_index]
        params = run_data.get('params')
        siteid = params.get('siteid')
        datadir = params.get('datadir')
        log = f'{run_uuid}.log'
        firstyear = params.get('firstyear')
        lastyear = params.get('lastyear')
        custom_params = run_data.get('custom_params')
        custom_params_str = ''
        if custom_params:
            for k, v in custom_params.items():
                custom_params_str += f'--{k} {v} '
        process_pid = None
        process = subprocess.Popen(['bash', script_path,
                                    self.oneflux_path,
                                    self.command,
                                    Path(self.oneflux_path)/'data',
                                    siteid,
                                    datadir,
                                    str(firstyear), str(lastyear),
                                    Path(self.oneflux_path)/log,
                                    self.matlab_path,
                                    custom_params_str], preexec_fn=os.setsid)
        process_pid = process.pid
        return process_pid, run_uuid


if __name__ == '__main__':
    # read config file
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    
    run_name = config.get('run_name')
    machine_name = config.get('machine_name')
    load_dotenv()
    gh_token = os.environ.get('TOKEN')
    gh_repo = os.environ.get('REPO')
    gh_branch = os.environ.get('BRANCH')
    
    flow_manager = FlowManager(gh_token, gh_repo, gh_branch, machine_name)
    scenario_log_df, status_str = flow_manager.get_scenario_log()
    log.info(status_str)
    machine_log_df, status_str = flow_manager.get_machine_log()
    log.info(status_str)
    
    # update machine log with step 0
    machine_log_df, status_str = flow_manager.run_step_0(scenario_log_df, machine_log_df)
    log.info(status_str)
    # consolidate df
    filtered_run_state_df, status_str = flow_manager.get_run_state(machine_log_df)
    log.info(status_str)
    
    # filtered_run_state_df = machine_log_df
    # run step 04 -> 1
    machine_log_df, status_str = flow_manager.run_step_04_1(filtered_run_state_df)
    log.info(status_str)
    
    machine_log_df, status_str = flow_manager.run_step_1_234(machine_log_df)
    log.info(status_str)
