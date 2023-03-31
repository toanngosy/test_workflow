import wandb
from datetime import datetime
import os

PROJECT_WANDB = 'test_workflow'

now = datetime.now()
api = wandb.Api()
latest_report = api.reports(path=PROJECT_WANDB).next()
latest_report_time = datetime.strptime(latest_report.updated_at, '%Y-%m-%dT%H:%M:%S')
if latest_report_time < now:
    os.sleep(10)
    latest_report = api.reports(path=PROJECT_WANDB).next()
    latest_report_time = datetime.strptime(latest_report.updated_at, '%Y-%m-%dT%H:%M:%S')
else:
    print(latest_report.url)
