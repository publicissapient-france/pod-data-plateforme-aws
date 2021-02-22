import boto3
import os
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
	sf_client = boto3.client('stepfunctions')
	state_machine_arn = os.environ['STATEMACHINEARN']
	
	event_key = event['Records'][0]['s3']['object']['key']
	filename = os.path.basename(event_key)
	source_name = event_key.split("/")[0]
	table_name = os.path.splitext(filename)[0]

	sf_input = json.dumps({
		"rawBucketName": os.environ['RAWBUCKETNAME'],
		"preparedBucketName": os.environ['PREPAREDBUCKETNAME'],
		"schemaBucketName": os.environ['SCHEMABUCKETNAME'],
		"sourceName": source_name,
		"tableName": table_name,
	})
	logger.info(f'Step Function trigger input: {sf_input}')

	response = sf_client.start_execution(
		stateMachineArn=state_machine_arn,
		input=sf_input
	)
	logger.info(f'Step Function trigger response: {response}')
