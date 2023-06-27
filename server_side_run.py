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

MACHINE_STATUS_FILE = 'machine_status.csv'
MACHINE_LOG_FILE = 'log.csv'


def generate_content(result, run_id):
    content = ''
    content += f'{run_id}'
    content += '\n\n'
    content += str(random.randint(1, result))
    return content


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


def _get_report(repo, github_branch, github_report_path):
    try:
        content = repo.get_contents(github_report_path, ref=github_branch)
        file_sha = content.sha
        file_last_modified = content.last_modified
        print(f'File {github_report_path} exists in branch {github_branch}.')
    except:
        file_sha = None
        file_last_modified = None
        print(f'File {github_report_path} does not exist in branch {github_report_path}. Creating new file...')
    return file_sha, file_last_modified


def run_and_push_report(func, run_id, *args, **kwargs): 
    _create_or_get_branch(repo, github_branch)
    file_sha, _ = _get_report(repo, github_branch, github_report_path)

    # update or create file content
    result = func(*args, **kwargs)
    new_content = generate_content(result, run_id)
    report_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not file_sha:
        file_status = repo.create_file(github_report_path, f'generate report {report_time}', new_content, branch=github_branch)
        print(f'New file {github_report_path} created in branch {github_branch}.')
    else:
        file_status = repo.update_file(github_report_path, f'update report {report_time}', new_content, file_sha, branch=github_branch)
        print(f'File {github_report_path} updated in branch {github_branch}.')
    new_file_sha = file_status.get('commit').sha
    return new_file_sha

    
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

    updated_by = f'{machine_name} server'
    github_machine_status_path = MACHINE_STATUS_FILE
    try:
        github_machine_status_contents = repo.get_contents(github_machine_status_path,
                                                           ref=github_branch)
        file_sha = github_machine_status_contents.sha
        content = github_machine_status_contents.content
        github_machine_status_data = base64.b64decode(content).decode('utf-8')
    except:
        file_sha = None
        github_machine_status_data = ''
    
    change_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if file_sha:
        github_machine_status_df = pd.read_csv(io.StringIO(github_machine_status_data))
        if machine_name in github_machine_status_df.machine_name.values:
            machine_info = (github_machine_status_df
                            .query(f'machine_name == \'{machine_name}\''))
            machine_state = (machine_info
                             .status
                             .values[0])
            if machine_state == 1:
                github_machine_status_df.loc[github_machine_status_df.machine_name == machine_name,
                                             'updated_by'] = updated_by
                github_machine_status_df.loc[github_machine_status_df.machine_name == machine_name,
                                             'last_updated_timestamp'] = change_time
                github_machine_status_df.loc[github_machine_status_df.machine_name == machine_name,
                                             'status'] = 0
                updated_content = github_machine_status_df.to_csv(index=False)
                file_status = repo.update_file(github_machine_status_path,
                                               f'generate machine status',
                                               updated_content,
                                               file_sha,
                                               branch=github_branch)
                
                ##############################################################
                # add to machine-specific .log to pending
                machine_log_file = MACHINE_LOG_FILE
                github_machine_log_path = f'report/{machine_name}/{machine_log_file}'
                try:
                    github_machine_log_contents = repo.get_contents(github_machine_log_path,
                                                                    ref=github_branch)
                    file_sha = github_machine_log_contents.sha
                    content = github_machine_log_contents.content
                    github_machine_log_data = base64.b64decode(content).decode('utf-8')
                except:
                    file_sha = None
                    github_machine_log_data = ''
                
                updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                run_id = machine_info.run_id.values[0]
                actor = machine_info.actor.values[0]
                machine_state = 'pending'
                additional_info = 'executing'
                if not file_sha:
                    csv_headers = 'run_id,actor,last_updated_timestamp,state,process_id,additional_info\n'

                    updated_content = (f'{csv_headers}'
                                       f'{run_id},{actor},{updated_time},{machine_state},,{additional_info}')
                    file_status = repo.create_file(github_machine_log_path,
                                                f'generate machine status',
                                                updated_content,
                                                branch=github_branch)
                else:
                    github_machine_log_df = pd.read_csv(io.StringIO(github_machine_log_data))
                    new_log = {'run_id': run_id, 
                               'actor': actor,
                               'last_updated_timestamp': updated_time,
                               'state': machine_state}
                    updated_content = github_machine_log_df.to_csv(index=False)
                    file_status = repo.update_file(github_machine_log_path,
                                                f'generate machine status',
                                                updated_content,
                                                file_sha,
                                                branch=github_branch)
            else:
                # check if there is any running process, check if process finish or not
                # if finish, render and push result
                # update log file
                machine_log_file = MACHINE_LOG_FILE
                github_machine_log_path = f'report/{machine_name}/{machine_log_file}'
                try:
                    github_machine_log_contents = repo.get_contents(github_machine_log_path,
                                                                    ref=github_branch)
                    file_sha = github_machine_log_contents.sha
                    content = github_machine_log_contents.content
                    github_machine_log_data = base64.b64decode(content).decode('utf-8')
                except:
                    file_sha = None
                    github_machine_log_data = ''
                if file_sha:
                    github_machine_log_df = pd.read_csv(io.StringIO(github_machine_log_data))
                    github_machine_log_pending_df = github_machine_log_df.query('state == \'pending\' and additional_info == \'executing\'')
                    for row in github_machine_log_pending_df.itertuples():
                        process = subprocess.Popen(['python', './intensive_process.py', str(row.run_id)], preexec_fn=os.setsid)
                        process.communicate()
                        if process.poll() is None or process.poll() == 0:
                            updated_additional_info = 'done'
                            github_machine_log_df.loc[row.Index, 'additional_info'] = updated_additional_info
                            
                            updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            state = 'running'
                            process_id = process.pid
                            addtional_info = 'executing'
                            new_row = pd.DataFrame({
                                'run_id': [row.run_id],
                                'actor': [row.actor],
                                'last_updated_timestamp': [updated_time],
                                'state': [state],
                                'process_id': [process_id],
                                'additional_info': [addtional_info]
                            })
                            github_machine_log_df = pd.concat([github_machine_log_df, new_row])

                    github_machine_log_running_df = github_machine_log_df.query('state == \'running\' and additional_info == \'executing\'')
                    for row in github_machine_log_running_df.itertuples():
                        files = Path('./result').glob("*")
                        file_names = [file.name for file in files if file.is_file()]
                        if f'output_{row.run_id}.txt' in file_names:
                            updated_additional_info = 'done'
                            github_machine_log_df.loc[row.Index, 'additional_info'] = updated_additional_info
                            is_failed = os.path.getsize(f'./result/error_{row.run_id}.txt') != 0
                            if is_failed:
                                state = 'failed'
                                content_file = f'./result/error_{row.run_id}.txt' 
                            else:
                                state = 'suceeded'
                                content_file = f'./result/output_{row.run_id}.txt'
                            
                            with open(content_file, 'r') as f:
                                content = f.read()
                            
                            file_status = repo.create_file(f'report/{machine_name}/REPORT_{row.run_id}.md', f'generate report {row.run_id}', content, branch=github_branch)
                            updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            new_row = pd.DataFrame({
                                'run_id': [row.run_id],
                                'actor': [row.actor],
                                'last_updated_timestamp': [updated_time],
                                'state': [state],
                                'process_id': [row.process_id],
                                'additional_info': [f'report/{machine_name}/REPORT_{row.run_id}.md']
                            })
                            github_machine_log_df = pd.concat([github_machine_log_df, new_row])
                    
                    github_machine_log_df = github_machine_log_df.sort_values(by='last_updated_timestamp', ascending=False)
                    updated_content = github_machine_log_df.to_csv(index=False)
                    file_status = repo.update_file(github_machine_log_path,
                                                   f'update log for machine {machine_name}',
                                                   updated_content,
                                                   file_sha,
                                                   branch=github_branch)
                    
                            
                            
                            
                        

    