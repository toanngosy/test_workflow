import datetime
import time
from dotenv import load_dotenv
import os
import pytz
from github import Github, GithubException
import sys

def create_or_get_branch(repo, github_branch):
    try:
        branch = repo.get_branch(github_branch)
    except GithubException:
        branch = None
        
    if branch is None:
        base_branch = repo.get_branch(repo.default_branch)
        base_commit = repo.get_commit(base_branch.commit.sha)
        # create new branch at the head commit of the default branch
        repo.create_git_ref(f'refs/heads/{github_branch}', base_commit.sha)


def get_report(repo, github_branch, github_report_path):
    try:
        content = repo.get_contents(github_report_path, ref=github_branch)
        file_sha = content.sha
        latest_commit_sha = repo.get_commits(sha=github_branch)[0].sha
        file_last_modified = content.last_modified
        
        content_str = content.decoded_content.decode()
        run_id = content_str.split('\n')[0]
    except:
        file_sha = None
        file_last_modified = None
        latest_commit_sha = None
        run_id = None
    return latest_commit_sha, file_last_modified, run_id


if __name__ == '__main__':
    github_report_machine = sys.argv[1].split('/')[1]
    load_dotenv()
    github_token = os.environ.get('TOKEN')
    github_repo = os.environ.get('REPO')
    github_branch = os.environ.get('BRANCH')
    github_report_path = os.environ.get('REPORT_PATH').format(github_report_machine)
    g = Github(github_token)
    repo = g.get_repo(github_repo)
    
    create_or_get_branch(repo, github_branch)
    
    file_sha, file_last_modified, run_id = get_report(repo, github_branch, github_report_path)
    
    print(f'The report for run_id {run_id} is ready to view. See it at {github_report_path}')
    