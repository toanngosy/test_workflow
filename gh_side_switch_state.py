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
from uuid import uuid4


MACHINE_STATUS_FILE = 'run_status.csv'
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


def change_machine_status(repo, github_branch, actor, machine_name, file_path):
    _create_or_get_branch(repo, github_branch)
    updated_by = UPDATED_BY
    machine_log_file = MACHINE_STATUS_FILE
    github_machine_log_path = f'report/{machine_log_file}'
    try:
        github_machine_status_contents = repo.get_contents(github_machine_log_path,
                                                           ref=github_branch)
        file_sha = github_machine_status_contents.sha
        content = github_machine_status_contents.content
        github_machine_status_data = base64.b64decode(content).decode('utf-8')
    except:
        file_sha = None
        github_machine_status_data = ''
    
    updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    file_path_list = file_path.split()
    if not file_sha:
        csv_headers = 'uuid,actor,last_updated_timestamp,updated_by,file_path\n'
        updated_content = f'{csv_headers}'
        for f in file_path_list:
            uuid = str(uuid4())
            updated_content += f'{uuid},{actor},{updated_time},{updated_by},{f}\n'
        updated_content.strip('\n')
        status_str = f'Set all machines to run at {updated_time}.'
        file_status = repo.create_file(github_machine_log_path,
                                       f'run_status.csv is generated, add new run request.',
                                       updated_content,
                                       branch=github_branch)
    else:
        github_machine_status_df = pd.read_csv(io.StringIO(github_machine_status_data))
        status_str = f'run_status.csv is already created, add new run request.'
        new_log = {}
        for f in file_path_list:
            new_log.setdefault('uuid', []).append(str(uuid4()))
            new_log.setdefault('actor', []).append(actor)
            new_log.setdefault('last_updated_timestamp', []).append(updated_time)
            new_log.setdefault('updated_by', []).append(updated_by)
            new_log.setdefault('file_path', []).append(f)
        github_machine_status_df = pd.concat([pd.DataFrame(new_log), github_machine_status_df])
        updated_content = github_machine_status_df.to_csv(index=False)
        file_status = repo.update_file(github_machine_log_path,
                                       f'add new run request',
                                       updated_content,
                                       file_sha,
                                       branch=github_branch)
    new_file_sha = file_status.get('commit').sha
    return new_file_sha, status_str


if __name__ == '__main__':
    actor, machine_name, file_path = sys.argv
    github_token = os.environ.get('TOKEN')
    github_repo = os.environ.get('REPO')
    github_branch = os.environ.get('BRANCH')
    g = Github(github_token)
    repo = g.get_repo(github_repo)
    _, status_str = change_machine_status(repo, github_branch, actor, machine_name, file_path)
    
    print(f'Status: {status_str}')
