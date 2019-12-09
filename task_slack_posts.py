"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""
from tulflow import tasks

def slackpostonsuccess(**context):
    """Task to Post Successful FunCake DAG Completion on Aggregator Slack."""
    msg = None
    return tasks.execute_slackpostonsuccess(context, conn_id="FUNCAKE_SLACK_WEBHOOK", message=msg)

def slackpostonfail(context):
    """Task Method to Post Failed Task on Aggregator Slack."""
    msg = None
    return tasks.execute_slackpostonfail(context, conn_id="FUNCAKE_SLACK_WEBHOOK", message=msg)
