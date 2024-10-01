#!/bin/bash
set -e -u

# uncomment to debug
# set -x

# Run an ECS Task from the provided CLUSTER and SERVICE

# required environment varialbes
# - CLUSTER
# - SERVICE

# Get the Security Groups that can run the task.
echo "Retrieving SecurityGroups for SERVICE:${SERVICE} in CLUSTER:${CLUSTER}"
SECURITY_GROUPS=$(aws ecs describe-services \
  --services $SERVICE \
  --cluster $CLUSTER \
  --query services[0].networkConfiguration.awsvpcConfiguration.securityGroups \
  --output text \
  | sed 's/\t/,/g')
echo "SECURITY GROUPS: ${SECURITY_GROUPS}"

# Get the Subnets that the task runs on.
echo "Retrieving subnets for SERVICE:${SERVICE} in CLUSTER:${CLUSTER}"
SUBNETS=$(aws ecs describe-services \
  --services $SERVICE \
  --cluster $CLUSTER \
  --query services[0].networkConfiguration.awsvpcConfiguration.subnets \
  --output text \
  | sed 's/\t/,/g')
echo "SUBNETS: ${SUBNETS}"

# Run the ECS task
aws ecs run-task \
  --cluster $CLUSTER \
  --task-definition $SERVICE \
  --launch-type FARGATE \
  --count 1 \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SECURITY_GROUPS],assignPublicIp=DISABLED}"
