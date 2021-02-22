#! /bin/bash

function deploy_s3() {
  ENVIRONMENT=$1

  echo "Creating stack $STACK_NAME for envrionment $ENV"
  aws cloudformation deploy \
      --template-file infra/S3-bucket-config.yaml \
      --stack-name "$ENVIRONMENT-pod-dp-s3" \
      --parameter-overrides Environment="$ENVIRONMENT"
}


function deploy_ingestion() {
  PROFILE=$1
  ENVIRONMENT=$2

  ARTIFACT_BUCKET_NAME=$(AWS_PROFILE=$PROFILE aws cloudformation describe-stacks --stack-name prd-pod-dp-s3 --output text --query 'Stacks[0].Outputs[?OutputKey==`ArtifactsBucket`].OutputValue')
  STACK_NAME="$ENVIRONMENT-pod-dp-ingestion"

  echo "Creating stack $STACK_NAME for envrionment $ENV. Artifact bucket: $ARTIFACT_BUCKET_NAME"
  AWS_PROFILE=$PROFILE aws cloudformation deploy \
      --template-file infra/ingestion-pipeline.yaml \
      --stack-name "$STACK_NAME" \
      --capabilities CAPABILITY_NAMED_IAM \
      --parameter-overrides \
          Environment="$ENVIRONMENT" \
          S3Stack="$ENVIRONMENT-pod-dp-s3" \
          ETLScriptsPrefix="glue" \
          JobLanguage="scala" \
          ArtifactsBucket="$ARTIFACT_BUCKET_NAME"  \
          IncomingBucketName="pod-dp-$ENVIRONMENT-incoming"
}
