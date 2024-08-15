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
        csv_header = 'uuid,index,actor,file_path,last_updated_timestamp,state,process_id,additional_info\n'
        get_log_str = f'Get log.csv of machine: {self.machine_name}'
        create_new_log_str = f'Create initial log.csv for {self.machine_name} ' + 'at ' + '{updated_time}'
        machine_log_df, status_str = self._get_log(self.machine_log_path,
                                                   csv_header,
                                                   create_new_log_str,
                                                   get_log_str)
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
                new_machine_runs = scenario.get('runs')
                for run_index, _ in enumerate(new_machine_runs):
                    updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    additional_info = 'pending'
                    new_machine_log.setdefault('uuid', []).append(uuid)
                    new_machine_log.setdefault('index', []).append(run_index)
                    new_machine_log.setdefault('actor', []).append(actor)
                    new_machine_log.setdefault('file_path', []).append(file_path)
                    new_machine_log.setdefault('last_updated_timestamp', []).append(updated_time)
                    new_machine_log.setdefault('state', []).append(current_state)
                    new_machine_log.setdefault('additional_info', []).append(additional_info)
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

def get_run_state_dict():
    pass

def step_0_1():
    # check if machine is available, start the run
    # add run to the log, change state from 0 -> 1
    # if cannot start the run, don't add
    pass

def step_1_2():
    # check if run is finish, add run to the log, change state from 1 -> 2
    # if not finish, do nothing
    pass

def step_2_3():
    # check status of the run and upload the data
    pass


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
    # TODO: flow_manager log here
    print(status_str)
    machine_log_df, status_str = flow_manager.get_machine_log()
    # TODO: flow_manager log here
    print(status_str)
    
    # update machine log with step 0
    machine_log_df, status_str = flow_manager.run_step_0(scenario_log_df, machine_log_df)
    print(status_str)
    
    # run_state_dict = flow_manager.get_run_state_dict(scenario_log_df, machine_log_df)
    
    print()
    
'''
if __name__ == '__main__':
    # read config file
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    
    run_name = config.get('run_name')
    machine_name = config.get('machine_name')
    load_dotenv()
    github_token = os.environ.get('TOKEN')
    github_repo = os.environ.get('REPO')
    github_branch = os.environ.get('BRANCH')
    g = Github(github_token)
    repo = g.get_repo(github_repo)

    # create a machine-level log file
    github_machine_log_path = f'report/server/{machine_name}/log.csv'
    try:
        github_machine_log_contents = repo.get_contents(github_machine_log_path,
                                                        ref=github_branch)
        file_sha = github_machine_log_contents.sha
        content = github_machine_log_contents.content
        github_machine_log_data = base64.b64decode(content).decode('utf-8')
        
    except:
        csv_headers = 'uuid,index,actor,file_path,last_updated_timestamp,state,process_id,additional_info\n'
        updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status_str = f'Create initial log.csv for {machine_name} at {updated_time}.'
        file_status = repo.create_file(github_machine_log_path,
                                       f'generate log.csv for machine {machine_name}',
                                       csv_headers,
                                       branch=github_branch)
        time.sleep(1)
        github_machine_log_contents = repo.get_contents(github_machine_log_path,
                                                        ref=github_branch)
        file_sha = github_machine_log_contents.sha
        content = github_machine_log_contents.content
        github_machine_log_data = base64.b64decode(content).decode('utf-8')
    github_machine_log_df = pd.read_csv(io.StringIO(github_machine_log_data))
    ##################################################################

    # read the global run status file
    run_status_file = RUN_STATUS_FILE
    github_run_status_path = f'report/{run_status_file}'
    try:
        github_run_status_contents = repo.get_contents(github_run_status_path,
                                                       ref=github_branch)
        file_status_sha = github_run_status_contents.sha
        content = github_run_status_contents.content
        github_run_status_data = base64.b64decode(content).decode('utf-8')
        github_run_status_df = pd.read_csv(io.StringIO(github_run_status_data))
    except:
        file_status_sha = None
        github_run_status_data = ''
    ######################################################################
    
    # add new run to machine log
    for uuid in (set(github_run_status_df.uuid.tolist()) - set(github_machine_log_df.uuid.tolist())):
        data = github_run_status_df[github_run_status_df['uuid'] == uuid]
        file_path = data['file_path'].values[0]
        actor = data['actor'].values[0]
        run_file_contents = repo.get_contents(file_path, ref=github_branch)
        run_file_data = run_file_contents.decoded_content.decode('utf-8')
        run_file_yaml = yaml.safe_load(run_file_data)
        runs = run_file_yaml[0]
        new_log = {}
        if runs.get('machine') == machine_name:
            runs = runs.get('runs')
            for i, run in enumerate(runs):
                run_index = i
                updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                additional_info = 'pending'
                new_log.setdefault('uuid', []).append(uuid)
                new_log.setdefault('index', []).append(run_index)
                new_log.setdefault('actor', []).append(actor)
                new_log.setdefault('file_path', []).append(file_path)
                new_log.setdefault('last_updated_timestamp', []).append(updated_time)
                new_log.setdefault('state', []).append(0)
                new_log.setdefault('additional_info', []).append('pending')

            github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
            updated_content = github_machine_log_df.to_csv(index=False)
            file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
            file_status = repo.update_file(github_machine_log_path,
                                            f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
                                            updated_content,
                                            file_sha,
                                            branch=github_branch)
            time.sleep(1)
    ################ done here ############################
    # read machine log to see if any pending
    uuids_with_only_status_0 = github_machine_log_df.groupby(['uuid', 'index']).filter(lambda x: set(x['state']) == {0})
    unique_uuids = uuids_with_only_status_0['uuid'].unique().tolist()
    # pending uuid
    data = uuids_with_only_status_0.iloc[0]
    uuid = data['uuid']
    file_path = data['file_path']
    param_index = data['index']
    actor = data['actor']
    run_file_contents = repo.get_contents(file_path, ref=github_branch)
    run_file_data = run_file_contents.decoded_content.decode('utf-8')
    run_file_yaml = yaml.safe_load(run_file_data)
    runs = run_file_yaml[0].get('runs')
    run_data = runs[param_index]
    params = run_data.get('params')
    siteid = params.get('siteid')
    datadir = params.get('datadir')
    log = f'{uuid}-{param_index}.log'
    firstyear = params.get('firstyear')
    lastyear = params.get('lastyear')
    custom_params = run_data.get('custom_params')
    custom_params_str = ''

    with open(server_config_path, 'r') as file:
        data = yaml.safe_load(file)
    oneflux_path = data.get('oneflux_path')
    command = data.get('command')
    matlab_path = data.get('matlab_path')
    run = data.get('oneflux_run')

    if custom_params:
        for k, v in custom_params.items():
            custom_params_str += f'--{k} {v} '
    process = subprocess.Popen(['bash', script_path,
                                oneflux_path,
                                command,
                                Path(oneflux_path)/'data',
                                siteid,
                                datadir,
                                str(firstyear), str(lastyear),
                                Path(oneflux_path)/log,
                                matlab_path,
                                custom_params_str], preexec_fn=os.setsid)
    # try to run first round
    # process = subprocess.Popen(['bash', script_path], preexec_fn=os.setsid)
    # if process is 'running', then add a line to the log that process is running     
    updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    machine_state = 1
    process_id = process.pid
    additional_info = 'running'
    new_log = {
        'uuid': [uuid],
        'index': [param_index],
        'actor': [actor],
        'last_updated_timestamp': [updated_time],
        'state': [machine_state],
        'process_id': [process_id],
        'additional_info': [additional_info]
    }
    github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
    updated_content = github_machine_log_df.to_csv(index=False)
    file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    file_status = repo.update_file(github_machine_log_path,
                                    f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
                                    updated_content,
                                    file_sha,
                                    branch=github_branch)
    time.sleep(1)
    file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    # check if process is finish
    process.communicate()
    if process.poll() is None or process.poll() == 0:
        updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        machine_state = 2
        process_id = process.pid
        additional_info = 'done'
        new_log = {
                'uuid': [uuid],
                'index': [param_index],
                'actor': [actor],
                'last_updated_timestamp': [updated_time],
                'state': [machine_state],
                'process_id': [process_id],
                'additional_info': [additional_info]
        }
        github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
        updated_content = github_machine_log_df.to_csv(index=False)
        file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
        file_status = repo.update_file(github_machine_log_path,
                                        f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
                                        updated_content,
                                        file_sha,
                                        branch=github_branch)
        time.sleep(1)
        file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
        # write/push result
        content_file = Path(oneflux_path)/log
        with open(content_file, 'r') as f:
            content = f.read()
        
        machine_state = 3
        file_status = repo.create_file(f'report/{siteid}/{uuid}/{param_index}/REPORT.log', f'generate report {uuid}', content, branch=github_branch)
        updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        additional_info = f'report/{siteid}/REPORT_{uuid}.log'
        
        output_img_path = Path('/home/portnoy/u0/sytoanngo/ONEFlux/data/US-ARc_sample_input/99_fluxnet2015')
        png_files = list(output_img_path.glob('*.png'))
        element_list = list()
        master_ref = repo.get_git_ref(f'heads/{github_branch}')
        master_sha = master_ref.object.sha
        base_tree = repo.get_git_tree(master_sha)
        commit_message = 'test upload images'
        
        for entry in png_files:
            path_in_repo = Path(entry).name
            entry = str(entry)
            with open(entry, 'rb') as input_file:
                data = input_file.read()
            if entry.endswith('.png'):
                data = base64.b64encode(data).decode('utf-8')
            blob = repo.create_git_blob(data, 'base64')
            element = InputGitTreeElement(path=f'report/{siteid}/{uuid}/{param_index}/{path_in_repo}', mode='100644', type='blob', sha=blob.sha)
            element_list.append(element)
        tree = repo.create_git_tree(element_list, base_tree)
        parent = repo.get_git_commit(master_sha)
        commit = repo.create_git_commit(commit_message, tree, [parent])
        master_ref.edit(commit.sha)

        new_log = {
            'uuid': [uuid],
            'index': [param_index],
            'actor': [actor],
            'last_updated_timestamp': [updated_time],
            'state': [machine_state],
            'process_id': [process_id],
            'additional_info': [additional_info]
        }
        github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
        updated_content = github_machine_log_df.to_csv(index=False)
        file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
        file_status = repo.update_file(github_machine_log_path,
                                        f'update log for machine {machine_name}',
                                        updated_content,
                                        file_sha,
                                        branch=github_branch)
    
#########################################################
    # 0: pending
    # 1: running
    # 2: done
    # 3: succeed/failed
    # if not file_status_sha:
    #     pass
    # else:
    #     github_machine_log_contents = repo.get_contents(github_machine_log_path,
    #                                                     ref=github_branch)
    #     file_sha = github_machine_log_contents.sha
    #     content = github_machine_log_contents.content
    #     github_machine_log_data = base64.b64decode(content).decode('utf-8')
    #     github_machine_log_df = pd.read_csv(io.StringIO(github_machine_log_data))

    #     for new_uuid in (set(github_machine_status_df.uuid.tolist()) - set(github_machine_log_df.uuid.tolist())): # this case we dont even have pending status
    #         status_str = f'Machine: {machine_name} is requested to run. Start to run...'
    #         run_data = github_machine_status_df[github_machine_status_df.uuid == new_uuid].iloc[0]
    #         uuid = run_data.uuid
    #         server_uuid = str(uuid4())
    #         actor = run_data.actor
    #         updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #         machine_state = 0
    #         process_id = None
    #         additional_info = 'pending'
    #         new_log = {'uuid': [uuid],
    #                    'server_uuid': [server_uuid],
    #                    'actor': [actor],
    #                    'last_updated_timestamp': [updated_time],
    #                    'state': [machine_state],
    #                    'process_id': [process_id],
    #                    'additional_info': [additional_info]}
    #         github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
    #         updated_content = github_machine_status_df.to_csv(index=False)
    #         file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    #         file_status = repo.update_file(github_machine_log_path,
    #                                        f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
    #                                        updated_content,
    #                                        file_sha,
    #                                        branch=github_branch)
    #         time.sleep(1)
    ################################### done here ########################################
    #         file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
            
    #         with open(file_path, 'r') as file:
    #             data = yaml.safe_load(file)
            
    #         oneflux_path = data.get('oneflux_path')
    #         command = data.get('command')
    #         matlab_path = data.get('matlab_path')
    #         run = data.get('oneflux_run')
    #         run_id = next(iter(run))
    #         run_data = run.get(run_id)
    #         params = run_data.get('params')
    #         siteid = params.get('siteid')
    #         datadir = params.get('datadir')
    #         log = params.get('log')
    #         firstyear = params.get('firstyear')
    #         lastyear = params.get('lastyear')
    #         custom_params = run_data.get('custom_params')
    #         custom_params_str = ''
    #         if custom_params:
    #             for k, v in custom_params.items():
    #                 custom_params_str += f'--{k} {v} '
    #         process = subprocess.Popen(['bash', script_path,
    #                                     oneflux_path,
    #                                     command,
    #                                     Path(oneflux_path)/'data',
    #                                     siteid,
    #                                     datadir,
    #                                     str(firstyear), str(lastyear),
    #                                     log,
    #                                     matlab_path,
    #                                     custom_params_str], preexec_fn=os.setsid)
    #         # try to run first round
    #         # process = subprocess.Popen(['bash', script_path], preexec_fn=os.setsid)
    #         # if process is 'running', then add a line to the log that process is running     
    #         updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #         machine_state = 1
    #         process_id = process.pid
    #         additional_info = 'running'
    #         new_log = {
    #             'uuid': [uuid],
    #             'server_uuid': [server_uuid],
    #             'actor': [actor],
    #             'last_updated_timestamp': [updated_time],
    #             'state': [machine_state],
    #             'process_id': [process_id],
    #             'additional_info': [additional_info]
    #         }
    #         github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
    #         updated_content = github_machine_log_df.to_csv(index=False)
    #         file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    #         file_status = repo.update_file(github_machine_log_path,
    #                                        f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
    #                                        updated_content,
    #                                        file_sha,
    #                                        branch=github_branch)
    #         time.sleep(1)
    #         file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    #         # check if process is finish
    #         process.communicate()
    #         if process.poll() is None or process.poll() == 0:
    #             updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             machine_state = 2
    #             process_id = process.pid
    #             additional_info = 'done'
    #             new_log = {
    #                    'uuid': [uuid],
    #                    'server_uuid': [server_uuid],
    #                    'actor': [actor],
    #                    'last_updated_timestamp': [updated_time],
    #                    'state': [machine_state],
    #                    'process_id': [process_id],
    #                    'additional_info': [additional_info]
    #             }
    #             github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
    #             updated_content = github_machine_log_df.to_csv(index=False)
    #             file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    #             file_status = repo.update_file(github_machine_log_path,
    #                                            f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
    #                                            updated_content,
    #                                            file_sha,
    #                                            branch=github_branch)
    #             time.sleep(1)
    #             file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    #             # write/push result
    #             content_file = Path(oneflux_path)/log
    #             with open(content_file, 'r') as f:
    #                 content = f.read()
                
    #             machine_state = 3
    #             file_status = repo.create_file(f'report/{siteid}/{run_id}/REPORT.log', f'generate report {run_id}', content, branch=github_branch)
    #             updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             additional_info = f'report/{siteid}/REPORT_{run_id}.log'
                
    #             output_img_path = Path('/home/portnoy/u0/sytoanngo/ONEFlux/data/US-ARc_sample_input/99_fluxnet2015')
    #             png_files = list(output_img_path.glob('*.png'))
    #             element_list = list()
    #             master_ref = repo.get_git_ref(f'heads/{github_branch}')
    #             master_sha = master_ref.object.sha
    #             base_tree = repo.get_git_tree(master_sha)
    #             commit_message = 'test upload images'
                
    #             for entry in png_files:
    #                 path_in_repo = Path(entry).name
    #                 entry = str(entry)
    #                 with open(entry, 'rb') as input_file:
    #                     data = input_file.read()
    #                 if entry.endswith('.png'):
    #                     data = base64.b64encode(data).decode('utf-8')
    #                 blob = repo.create_git_blob(data, 'base64')
    #                 element = InputGitTreeElement(path=f'report/{siteid}/{run_id}/{path_in_repo}', mode='100644', type='blob', sha=blob.sha)
    #                 element_list.append(element)
    #             tree = repo.create_git_tree(element_list, base_tree)
    #             parent = repo.get_git_commit(master_sha)
    #             commit = repo.create_git_commit(commit_message, tree, [parent])
    #             master_ref.edit(commit.sha)

    #             new_log = {
    #                 'uuid': [uuid],
    #                 'server_uuid': [server_uuid],
    #                 'actor': [actor],
    #                 'last_updated_timestamp': [updated_time],
    #                 'state': [machine_state],
    #                 'process_id': [process_id],
    #                 'additional_info': [additional_info]
    #             }
    #             github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
    #             updated_content = github_machine_log_df.to_csv(index=False)
    #             file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
    #             file_status = repo.update_file(github_machine_log_path,
    #                                             f'update log for machine {machine_name}',
    #                                             updated_content,
    #                                             file_sha,
    #                                             branch=github_branch)
'''