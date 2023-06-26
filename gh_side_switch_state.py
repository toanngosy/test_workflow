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

def _create_or_get_branch(repo, github_branch):
    try:
        branch = repo.get_branch(github_branch)
    except GithubException:
        branch = None
        
    if branch is None:
        base_branch = repo.get_branch(repo.default_branch)
        base_commit = repo.get_commit(base_branch.commit.sha)
        repo.create_git_ref(f'refs/heads/{github_branch}', base_commit.sha)


def change_machine_status(repo, github_branch, machine_name):
    _create_or_get_branch(repo, github_branch)
    github_machine_status_path = f'{github_branch}/{MACHINE_STATUS_FILE}'
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
    if not file_sha:
        csv_headers = 'last_updated_timestamp,machine_name,status\n'
        machine_state = 1
        updated_content = (f'{csv_headers}'
                           f'{change_time},{machine_name},{machine_state}')
        status_str = ''
        
        # file_status = repo.create_file(github_machine_status_path,
        #                                f'generate machine status',
        #                                updated_content,
        #                                branch=github_branch)
    else:
        github_machine_status_df = pd.read_csv(io.StringIO(github_machine_status_data))
        if machine_name not in github_machine_status_df.machine_name:
            machine_state = 1
            status_str = f'Machine: {machine_name} not found in global status file. Adding new machine and flag to run.'
        else:
            machine_state = github_machine_status_df.query(f'machine_name == {machine_name}').status
            if machine_state == 1:
                status_str = f'Machine: {machine_name} is already flagged to run.'
                return None, status_str
            else:
                status_str = f'Machine: {machine_name} is not running. Switch state to run.'
        updated_content = (f'{github_machine_status_data}\n'
                           f'{change_time},{machine_name},{machine_state}')
        # file_status = repo.update_file(github_machine_status_path,
        #                                f'generate machine status',
        #                                updated_content,
        #                                file_sha,
        #                                branch=github_branch)
    # new_file_sha = file_status.get('commit').sha
    new_file_sha = ''
    return new_file_sha, status_str


if __name__ == '__main__':
    _, run_id, machine_name = sys.argv
    github_token = os.environ.get('TOKEN')
    github_repo = os.environ.get('REPO')
    github_branch = os.environ.get('BRANCH')
    g = Github(github_token)
    repo = g.get_repo(github_repo)
    _, status_str = change_machine_status(repo, github_branch, machine_name)
    
    print(f'Github Actions Run ID {run_id} status: {status_str}')