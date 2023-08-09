import datetime as dt
import json
import os
import random
import requests
import yaml

from dotenv import load_dotenv
from github import Github, GithubException
import base64
import pandas as pd
import io
import subprocess
import psutil
from pathlib import Path
from uuid import uuid4

MACHINE_LOG_FILE = 'log.csv'
MACHINE_STATUS_FILE = 'run_status.csv'


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
    with open('server_config.yaml') as f:
        config = yaml.safe_load(f)
    machine_name = config.get('machine_name')

    load_dotenv()
    github_token = os.environ.get('TOKEN')
    github_repo = os.environ.get('REPO')
    github_branch = os.environ.get('BRANCH')
    github_report_path = os.environ.get('REPORT_PATH').format(machine_name)
    g = Github(github_token)
    repo = g.get_repo(github_repo)

    machine_log_file = MACHINE_LOG_FILE
    github_machine_log_path = f'report/{machine_name}/{machine_log_file}'
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
        file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha

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
            run_data = github_machine_status_df.query(f'uuid == \'{new_uuid}\'')
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
            file_status = repo.update_file(github_machine_log_path,
                                           f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
                                           updated_content,
                                           file_sha,
                                           branch=github_branch)
            file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
            # try to run first round
            process = subprocess.Popen(['python', './intensive_process.py', str(uuid)], preexec_fn=os.setsid)
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
            file_status = repo.update_file(github_machine_log_path,
                                           f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
                                           updated_content,
                                           file_sha,
                                           branch=github_branch)
            file_sha = repo.get_contents(github_machine_log_df, ref=github_branch).sha
            # check if process is finish
            process.communicate()
            if process.poll() is None or process.poll() == 0:
                updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                machine_state = 2
                process_id = process.pid
                additional_info = 'done'
                new_log = {
                       'uuid': [uuid],
                       'actor': [actor],
                       'last_updated_timestamp': [updated_time],
                       'updated_by': [updated_by],
                       'machine_name': [machine_name],
                       'state': [machine_state],
                       'process_id': [process_id],
                       'additional_info': [additional_info]
                }
                github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
                updated_content = github_machine_log_df.to_csv(index=False)
                file_status = repo.update_file(github_machine_log_path,
                                               f'switch to {additional_info} for run uuid {uuid} at {machine_name}',
                                               updated_content,
                                               file_sha,
                                               branch=github_branch)
                file_sha = repo.get_contents(github_machine_log_path, ref=github_branch).sha
                # write/push result
                files = Path('./result').glob("*")
                file_names = [file.name for file in files if file.is_file()]
                if f'output_{uuid}.txt' in file_names:
                    is_failed = os.path.getsize(f'./result/error_{uuid}.txt') != 0
                    if is_failed:
                        run_state = 'failed'
                        content_file = f'./result/error_{uuid}.txt' 
                    else:
                        run_state = 'suceeded'
                        content_file = f'./result/output_{uuid}.txt'
                    
                    with open(content_file, 'r') as f:
                        content = f.read()
                    
                    machine_state = 3
                    file_status = repo.create_file(f'report/{machine_name}/REPORT_{uuid}_{server_uuid}.md', f'generate report {uuid} for {machine_name}', content, branch=github_branch)
                    updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    additional_info = f'report/{machine_name}/REPORT_{uuid}.md'
                    new_log = {
                       'uuid': [uuid],
                       'actor': [actor],
                       'last_updated_timestamp': [updated_time],
                       'updated_by': [updated_by],
                       'machine_name': [machine_name],
                       'state': [machine_state],
                       'process_id': [process_id],
                       'additional_info': [additional_info]
                    }
                    github_machine_log_df = pd.concat([pd.DataFrame(new_log), github_machine_log_df])
                    updated_content = github_machine_log_df.to_csv(index=False)
                    file_status = repo.update_file(github_machine_log_path,
                                                    f'update log for machine {machine_name}',
                                                    updated_content,
                                                    file_sha,
                                                    branch=github_branch)
        
        
