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


SCENARIO_FILE = 'scenario.csv'
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

def _get_log(repo, branch, log_path, csv_header, create_new_log_str, get_log_str):
    try:
        contents = repo.get_contents(log_path,
                                     ref=branch)
        file_sha = contents.sha
        content = contents.content
        status_str = get_log_str
        data = base64.b64decode(content).decode('utf-8')
        df = pd.read_csv(io.StringIO(data))
    except:
        updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status_str = create_new_log_str.format(updated_time=updated_time)
        file_status = repo.create_file(log_path,
                                        status_str,
                                        csv_header,
                                        branch=branch)
        time.sleep(1)
        contents = repo.get_contents(log_path,
                                    ref=branch)
        file_sha = contents.sha
        content = contents.content
        data = base64.b64decode(content).decode('utf-8')
    df = pd.read_csv(io.StringIO(data))
    return df, file_sha, status_str
    
def add_scenario(repo, github_branch, actor, file_path):
    _create_or_get_branch(repo, github_branch)
    log_path = f'report/{SCENARIO_FILE}'
    csv_header = 'uuid,actor,last_updated_timestamp,updated_by,file_path\n'
    create_new_log_str = 'Create new scenarios.csv'
    get_log_str = 'scenarios.csv is already created, add new run request at {updated_time}'
    scenario_log_df, file_sha, status_str = _get_log(repo, github_branch, log_path, csv_header, create_new_log_str, get_log_str)
    updated_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    file_path_list = file_path.split()
    new_log = {}
    for f in file_path_list:
        new_log.setdefault('uuid', []).append(str(uuid4()))
        new_log.setdefault('actor', []).append(actor)
        new_log.setdefault('last_updated_timestamp', []).append(updated_time)
        new_log.setdefault('updated_by', []).append(UPDATED_BY)
        new_log.setdefault('file_path', []).append(f)
    scenario_log_df = pd.concat([pd.DataFrame(new_log), scenario_log_df])
    updated_content = scenario_log_df.to_csv(index=False)
    file_status = repo.update_file(log_path,
                                   f'add new run request',
                                   updated_content,
                                   file_sha,
                                   branch=github_branch)
    new_file_sha = file_status.get('commit').sha
    return new_file_sha, status_str

if __name__ == '__main__':
    _, actor, file_path = sys.argv
    github_token = os.environ.get('TOKEN')
    github_repo = os.environ.get('REPO')
    github_branch = os.environ.get('BRANCH')
    g = Github(github_token)
    repo = g.get_repo(github_repo)
    _, status_str = add_scenario(repo, github_branch, actor, file_path)
    
    print(f'Status: {status_str}')
