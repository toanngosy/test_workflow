import requests
import json
import wandb.apis.reports as wr


RUN_GITHUB = 'Manual test'
PROJECT_WANDB = 'test_workflow'
def run_process():
    report = wr.Report(
    project=PROJECT_WANDB,
    title='test_workflow report',
    description='Result of the run will be shown here'
    )
    report.blocks = [
        wr.TableOfContents(),
        wr.H1("Text and images example"),
        wr.P("Lorem ipsum dolor sit amet. Aut laborum perspiciatis sit odit omnis aut aliquam voluptatibus ut rerum molestiae sed assumenda nulla ut minus illo sit sunt explicabo? Sed quia architecto est voluptatem magni sit molestiae dolores. Non animi repellendus ea enim internos et iste itaque quo labore mollitia aut omnis totam."),
        wr.Image('https://api.wandb.ai/files/telidavies/images/projects/831572/8ad61fd1.png', caption='Craiyon generated images'),
        wr.P("Et voluptatem galisum quo facilis sequi quo suscipit sunt sed iste iure! Est voluptas adipisci et doloribus commodi ab tempore numquam qui tempora adipisci. Eum sapiente cupiditate ut natus aliquid sit dolor consequatur?"),
    ]
    report.save()

if __name__ == '__main__':
    r = requests.get('https://api.github.com/repos/toanngosy/test_workflow/actions/runs')
    r.status_code
    workflow_runs = json.loads(r.text).get('workflow_runs', [])
    if len(workflow_runs):
        for run in workflow_runs:
            if run['name'] == RUN_GITHUB and run['status'] == 'queued':
                run_process()
                continue

    