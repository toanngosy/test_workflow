import datetime
import time
from dotenv import load_dotenv
import os
import pytz
from github import Github, GithubException
import sys
import datetime as dt
import io
import pandas as pd
import base64


MACHINE_STATUS_FILE = 'machine_status.csv'
UPDATED_BY = 'GH Actions'

def _create_or_get_branch(repo, github_branch):
    try:
        branch = repo.get_branch(github_branch)
    except GithubException:
        branch = None
        
    if branch is None:
        base_branch = repo.get_branch(repo.default_branch)
        base_commit = repo.get_commit(base_branch.commit.sha)
        repo.create_git_ref(f'refs/heads/{github_branch}', base_commit.sha)


def change_machine_status(repo, github_branch, run_id, actor, machine_name):
    _create_or_get_branch(repo, github_branch)
    updated_by = UPDATED_BY
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
    
    updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not file_sha:
        csv_headers = 'run_id,actor,last_updated_timestamp,updated_by,machine_name,status\n'
        machine_state = 1
        updated_content = (f'{csv_headers}'
                           f'{run_id},{actor},{updated_time},{updated_by},{machine_name},{machine_state}')
        status_str = f'Machines status file not created. Create and flag machine {machine_name} to run.'
        file_status = repo.create_file(github_machine_status_path,
                                       f'generate machine status',
                                       updated_content,
                                       branch=github_branch)
    else:
        github_machine_status_df = pd.read_csv(io.StringIO(github_machine_status_data))
        if machine_name not in github_machine_status_df.machine_name.values:
            machine_state = 1
            status_str = f'Machine: {machine_name} not found in global status file. Adding new machine and flag to run.'
        else:
            machine_state = github_machine_status_df.query(f'machine_name == \'{machine_name}\'').status.values[0]
            if machine_state == 1:
                status_str = f'Machine: {machine_name} is already flagged to run.'
                return None, status_str
            else:
                status_str = f'Machine: {machine_name} is not running. Switch state to run.'
        github_machine_status_df.loc[github_machine_status_df.machine_name == machine_name,
                                     'updated_by'] = updated_by
        github_machine_status_df.loc[github_machine_status_df.machine_name == machine_name,
                                     'last_updated_timestamp'] = updated_time
        github_machine_status_df.loc[github_machine_status_df.machine_name == machine_name,
                                     'status'] = 1
        updated_content = github_machine_status_df.to_csv(index=False)
        file_status = repo.update_file(github_machine_status_path,
                                       f'generate machine status',
                                       updated_content,
                                       file_sha,
                                       branch=github_branch)
    new_file_sha = file_status.get('commit').sha
    return new_file_sha, status_str


if __name__ == '__main__':
    _, run_id, actor, machine_name = sys.argv
    github_token = os.environ.get('TOKEN')
    github_repo = os.environ.get('REPO')
    github_branch = os.environ.get('BRANCH')
    g = Github(github_token)
    repo = g.get_repo(github_repo)
    _, status_str = change_machine_status(repo, github_branch, run_id, actor, machine_name)
    
    print(f'Github Actions Run ID {run_id} status: {status_str}')