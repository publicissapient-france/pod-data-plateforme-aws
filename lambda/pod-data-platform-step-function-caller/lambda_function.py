import boto3
import os
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
  stepfunction = boto3.client('stepfunctions')
  stateMachineArn = os.environ['STATEMACHINEARN']
  input = json.dumps(event)
  logger.info(f'Step Function trigger input: {input}')

  response = stepfunction.start_execution(
    stateMachineArn=stateMachineArn,
    input=input
  )
  logger.info(f'Step Function trigger response: {response}')