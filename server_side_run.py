import datetime as dt
import json
import os
import random
import requests
import yaml

from dotenv import load_dotenv
<<<<<<< Updated upstream
from github import Github, GithubException
=======
from github import Github, GithubException, InputGitTreeElement
import base64
import pandas as pd
import io
import subprocess
import psutil
from pathlib import Path
from uuid import uuid4
import time
from base64 import b64encode
import yaml
>>>>>>> Stashed changes


def run_data_intensive_process():
    result = random.randint(1, 100)
    return result


def generate_content(result, run_id):
    content = ''
    content += f'{run_id}'
    content += '\n\n'
    content += str(random.randint(1, result))
    return content

# oneflux_path = '/home/portnoy/u0/sytoanngo/ONEFlux'
# oneflux_input = '/home/portnoy/u0/sytoanngo/ONEFlux/data/US-ARc_sample_input'
# oneflux_log = 'test_run_name.log'
# script_path = '/home/portnoy/u0/sytoanngo/test_workflow/oneflux.sh'
# run_id = '283a5b57-5266-4cb4-ad84-990c6b69b3e2'
# site_id = 'US-ARc'
file_path = '/home/portnoy/u0/sytoanngo/test_workflow/oneflux_run.yaml'
script_path = '/home/portnoy/u0/sytoanngo/test_workflow/oneflux_run_template.sh'

def _create_or_get_branch(repo, github_branch):
    try:
        branch = repo.get_branch(github_branch)
    except GithubException:
        branch = None
        
    if branch is None:
        base_branch = repo.get_branch(repo.default_branch)
        base_commit = repo.get_commit(base_branch.commit.sha)
        # create new branch at the head commit of the default branch
        repo.create_git_ref(f'refs/heads/{github_branch}', base_commit.sha)

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
    github_report_path = os.environ.get('REPORT_PATH').format(machine_name)
    g = Github(github_token)
    repo = g.get_repo(github_repo)
    
    r = requests.get('https://api.github.com/repos/toanngosy/test_workflow/actions/runs')
    r.status_code
    workflow_runs = json.loads(r.text).get('workflow_runs', [])
    if len(workflow_runs):
        for run in workflow_runs:
            try:
                gh_run_name, gh_machine_name, gh_run_actor, gh_run_id = run['name'].split('_')                
                if (gh_run_name == run_name 
                    and gh_machine_name == machine_name
                    and run['status'] == 'in_progress'):
                        cancel_workflow(gh_run_id)
                        run_and_push_report(run_data_intensive_process, gh_run_id)
                        continue
            except ValueError:
                pass

    machine_log_file = MACHINE_LOG_FILE
    github_machine_log_path = f'report/server/{machine_name}/{machine_log_file}'
    try:
        github_machine_log_contents = repo.get_contents(github_machine_log_path,
                                                        ref=github_branch)
        file_sha = github_machine_log_contents.sha
        content = github_machine_log_contents.content
        github_machine_log_data = base64.b64decode(content).decode('utf-8')
    except:
        csv_headers = 'uuid,server_uuid,actor,last_updated_timestamp,state,process_id,additional_info\n'
        updated_content = (f'{csv_headers}')
        updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status_str = f'create initial log.csv for {machine_name} at {updated_time}.'
        file_status = repo.create_file(github_machine_log_path,
                                       f'generate log.csv for machine {machine_name}',
                                       updated_content,
                                       branch=github_branch)
        time.sleep(1)

    updated_by = f'{machine_name} server'
    machine_status_file = MACHINE_STATUS_FILE
    github_machine_status_path = f'report/{machine_status_file}'
    try:
        github_machine_status_contents = repo.get_contents(github_machine_status_path,
                                                           ref=github_branch)
        file_status_sha = github_machine_status_contents.sha
        content = github_machine_status_contents.content
        github_machine_status_data = base64.b64decode(content).decode('utf-8')
        github_machine_status_df = pd.read_csv(io.StringIO(github_machine_status_data))
    except:
        file_status_sha = None
        github_machine_status_data = ''

    # 0: pending
    # 1: running
    # 2: done
    # 3: succeed/failed
    if not file_status_sha:
        pass
    else:
        github_machine_log_contents = repo.get_contents(github_machine_log_path,
                                                        ref=github_branch)
        file_sha = github_machine_log_contents.sha
        content = github_machine_log_contents.content
        github_machine_log_data = base64.b64decode(content).decode('utf-8')
        github_machine_log_df = pd.read_csv(io.StringIO(github_machine_log_data))

        for new_uuid in (set(github_machine_status_df.uuid.tolist()) - set(github_machine_log_df.uuid.tolist())): # this case we dont even have pending status
            status_str = f'Machine: {machine_name} is requested to run. Start to run...'
            run_data = github_machine_status_df[github_machine_status_df.uuid == new_uuid].iloc[0]
            uuid = run_data.uuid
            server_uuid = str(uuid4())
            actor = run_data.actor
            updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            machine_state = 0
            process_id = None
            additional_info = 'pending'
            new_log = {'uuid': [uuid],
                       'server_uuid': [server_uuid],
                       'actor': [actor],
                       'last_updated_timestamp': [updated_time],
                       'state': [machine_state],
                       'process_id': [process_id],
                       'additional_info': [additional_info]}
            github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
            updated_content = github_machine_status_df.to_csv(index=False)
            file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
            file_status = repo.update_file(github_machine_log_path,
                                           f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
                                           updated_content,
                                           file_sha,
                                           branch=github_branch)
            time.sleep(1)
            file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
            
            with open(file_path, 'r') as file:
                data = yaml.safe_load(file)
            
            oneflux_path = data.get('oneflux_path')
            command = data.get('command')
            matlab_path = data.get('matlab_path')
            run = data.get('oneflux_run')
            run_id = next(iter(run))
            run_data = run.get(run_id)
            params = run_data.get('params')
            siteid = params.get('siteid')
            datadir = params.get('datadir')
            log = params.get('log')
            firstyear = params.get('firstyear')
            lastyear = params.get('lastyear')
            custom_params = run_data.get('custom_params')
            custom_params_str = ''
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
                                        log,
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
                'server_uuid': [server_uuid],
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
                       'server_uuid': [server_uuid],
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
                file_status = repo.create_file(f'report/{siteid}/{run_id}/REPORT.log', f'generate report {run_id}', content, branch=github_branch)
                updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                additional_info = f'report/{siteid}/REPORT_{run_id}.log'
                
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
                    element = InputGitTreeElement(path=f'report/{siteid}/{run_id}/{path_in_repo}', mode='100644', type='blob', sha=blob.sha)
                    element_list.append(element)
                tree = repo.create_git_tree(element_list, base_tree)
                parent = repo.get_git_commit(master_sha)
                commit = repo.create_git_commit(commit_message, tree, [parent])
                master_ref.edit(commit.sha)

                new_log = {
                    'uuid': [uuid],
                    'server_uuid': [server_uuid],
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
