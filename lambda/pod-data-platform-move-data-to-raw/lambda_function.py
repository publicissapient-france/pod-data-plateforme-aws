import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    s3 = boto3.resource('s3')

    try:
        event_key = event['key']
        filename = os.path.basename(event_key)
        source_name = event_key.split("/")[0]
        table_name = event_key.split("/")[1]

        incoming_bucket = os.environ['INCOMINGBUCKETNAME']
        raw_bucket = os.environ['RAWBUCKETNAME']

        copy_source = {
            'Bucket': incoming_bucket,
            'Key': event_key
        }
        s3.meta.client.copy(copy_source, raw_bucket, filename)
    except Exception as e:
        logger.error(f"Not able to copy {event_key} incoming file")
        s3.meta.client.copy({'Bucket': incoming_bucket, 'Key': event_key}, incoming_bucket, f'error/{event_key}')
    finally:
        logger.error(f"Deleting {event_key}")
        s3.Object(incoming_bucket, event_key).delete()

    return json.dumps({
        "sourceName": source_name,
        "tableName": table_name,
        "fileName": filename
	})
