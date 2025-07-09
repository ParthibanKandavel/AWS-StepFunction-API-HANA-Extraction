import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('stepfunctions')
    response = client.start_execution(
        stateMachineArn='arn:aws:states:your-region:your-account-id:stateMachine:YourStateMachine',
        name='etl-run-' + event['execution_id'],
        input=json.dumps(event)
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Step Function started!')
    }
