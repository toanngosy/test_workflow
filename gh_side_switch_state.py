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


if __name__ == '__main__':
    _, run_id, machine_name = sys.argv
    
    print(f'{run_id}, {machine_name}')