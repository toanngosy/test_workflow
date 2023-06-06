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
    except:
        file_sha = None
        file_last_modified = None
        latest_commit_sha = None
    return latest_commit_sha, file_last_modified


if __name__ == '__main__':
    #gmt = pytz.timezone('GMT')
    #now = datetime.datetime.now(gmt)
    #gmt = pytz.timezone('GMT')
    # now = datetime.datetime.now()
    # load_dotenv()
    # github_token = os.environ.get('TOKEN')
    # github_repo = os.environ.get('REPO')
    # github_branch = os.environ.get('BRANCH')
    # github_report_path = os.environ.get('REPORT_PATH')
    # g = Github(github_token)
    # repo = g.get_repo(github_repo)
    
    # create_or_get_branch(repo, github_branch)
    
    # file_sha, file_last_modified = get_report(repo, github_branch, github_report_path)
    # latest_report_time = None
    # if file_last_modified:
    #     #latest_report_time = datetime.datetime.strptime(file_last_modified, '%a, %d %b %Y %H:%M:%S GMT').astimezone(gmt)
    #     latest_report_time = datetime.datetime.strptime(file_last_modified, '%a, %d %b %Y %H:%M:%S GMT')
    # while True:
    #     if not latest_report_time or latest_report_time < now:
    #         time.sleep(10)
    #         file_sha, file_last_modified = get_report(repo, github_branch, github_report_path)
    #         latest_report_time = None
    #         if file_last_modified:
    #             #latest_report_time = datetime.datetime.strptime(file_last_modified, '%a, %d %b %Y %H:%M:%S GMT').astimezone(gmt)
    #             latest_report_time = datetime.datetime.strptime(file_last_modified, '%a, %d %b %Y %H:%M:%S GMT')
    #     else:
    #         file_url = f'https://github.com/{github_repo}/blob/{file_sha}/{github_report_path}'
    #         print(f'Report link: {file_url}')
    #         break
    print('this is the params: ', sys.argv[1])
    