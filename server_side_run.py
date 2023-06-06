import datetime as dt
import json
import os
import random
import requests
import yaml

from dotenv import load_dotenv
from github import Github, GithubException


def run_data_intensive_process():
    result = random.randint(1, 100)
    return result


def generate_content(result, run_id):
    content = ''
    content += '{run_id}'
    content += '\n'
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


def run_and_push_report(func, *args, **kwargs): 
    _create_or_get_branch(repo, github_branch)
    file_sha, _ = _get_report(repo, github_branch, github_report_path)

    # update or create file content
    result = func(*args, **kwargs)
    new_content = generate_content(result)
    report_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not file_sha:
        file_status = repo.create_file(github_report_path, f'generate report {report_time}', new_content, branch=github_branch)
        print(f'New file {github_report_path} created in branch {github_branch}.')
    else:
        file_status = repo.update_file(github_report_path, f'update report {report_time}', new_content, file_sha, branch=github_branch)
        print(f'File {github_report_path} updated in branch {github_branch}.')
    new_file_sha = file_status.get('commit').sha
    return new_file_sha


def cancel_workflow(id):
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    url = f'https://api.github.com/repos/{github_repo}/actions/runs/{id}/cancel'
    response = requests.post(url, headers=headers)
    return response.status_code
    
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
                        run_and_push_report(run_data_intensive_process)
                        continue
            except ValueError:
                pass

    