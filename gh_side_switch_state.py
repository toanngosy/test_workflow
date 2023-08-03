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


MACHINE_LOG_FILE = 'log.csv'
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


def change_machine_status(repo, github_branch, uuid, actor, machine_name):
    _create_or_get_branch(repo, github_branch)
    updated_by = UPDATED_BY
    machine_log_file = MACHINE_LOG_FILE
    github_machine_log_path = f'report/{machine_name}/{machine_log_file}'
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
    if not file_sha:
        csv_headers = 'uuid,actor,last_updated_timestamp,updated_by,machine_name,state,process_id,additional_info\n'
        machine_state = 1
        updated_content = (f'{csv_headers}'
                           f'{uuid},{actor},{updated_time},{updated_by},{machine_name},{machine_state},,')
        status_str = f'Log for machine: {machine_name} is not created. Create and flag machine {machine_name} to run.'
        file_status = repo.create_file(github_machine_log_path,
                                       f'generate log for machine: {machine_name}',
                                       updated_content,
                                       branch=github_branch)
    else:
        github_machine_status_df = pd.read_csv(io.StringIO(github_machine_status_data))
        machine_latest_status = github_machine_status_df.iloc[0]
        if machine_latest_status.machine_state == 1:
            status_str = f'Machine: {machine_name} is already flagged to run or occupied at the moment. Abort the request.'
            return None, status_str
        else:
            status_str = f'Machine: {machine_name} is free now, flag to run.'
            new_log = {'uuid': [uuid],
                       'actor': [actor],
                       'last_updated_timestamp': [updated_time],
                       'updated_by': [updated_by],
                       'machine_name': [machine_name],
                       'machine_state': [1],
                       'process_id': [None],
                       'additional_info': [None]}
            github_machine_status_df = pd.concat([github_machine_status_df, pd.DataFrame(new_log)])
        updated_content = github_machine_status_df.to_csv(index=False)
        file_status = repo.update_file(github_machine_status_df,
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