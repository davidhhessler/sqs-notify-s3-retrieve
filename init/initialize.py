import boto3
import json
import time

from util.encoder import DateTimeEncoder

# Activate service connections
client = boto3.client('cloudformation')

stack_name = 'scale-s3-test-%s' % int(time.time())
with open('cloudformation-s3-sns-sqs.json') as template:
    stack = client.create_stack(StackName=stack_name,
                                TemplateBody=template.read())

    waiter = client.get_waiter('stack_create_complete')
    print 'Waiting for CloudFormation stack creation complete notification.'
    waiter.wait(StackName=stack['StackId'])

    print 'Stack creation complete.'
    print 'Resources:\n%s' % json.dumps(client.list_stack_resources(StackName=stack['StackId']),
                                        indent=2, sort_keys=True,
                                        cls=DateTimeEncoder)
