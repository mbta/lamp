#!/bin/bash
set -e -u

# uncomment to debug
# set -x

# Run an ECS Task in the LAMP ECS Cluster

# required environment varialbes
# - TASK_NAME
# - ENVIRONMENT

# Get the VPC ID based on the VPC name
if [ "$ENVIRONMENT" == "staging" ]; then
    VPC_NAME="vpc-dev-ctd-itd"
elif [ "$ENVIRONMENT" == "prod" ]; then
    VPC_NAME="vpc-prod-ctd-itd"A
else
    echo "Invalid environment: $ENVIRONMENT"
    exit 1
fi

echo "Retrieving VPC ID for ${VPC_NAME}"

VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=tag:Name,Values=${VPC_NAME}" \
  --query "Vpcs[0].VpcId" \
  --output text)

echo "VPC ID: ${VPC_ID}"

# Get the subnet ID based on the VPC ID
echo "Retrieving SUBNET ID"

SUBNET_ID=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=${VPC_ID}" \
  --query "Subnets[0].SubnetId" \
  --output text)

echo "SUBNET ID: ${SUBNET_ID}"

# Get the Security Group Id that can run the task.
SG_NAME="lamp-${TASK_NAME}-${ENVIRONMENT}-ecs-service"
echo "Retrieving SECURITY GROUP ID for ${SG_NAME}"

SECURITY_GROUP_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=${SG_NAME}" \
  --query "SecurityGroups[0].GroupId" \
  --output text)

echo "SECURITY GROUP ID: ${SECURITY_GROUP_ID}"

# Build the Task Definition from the Task Name and Env
TASK_DEFINITION="lamp-${TASK_NAME}-${ENVIRONMENT}"

# Run the ECS task
aws ecs run-task \
  --cluster lamp \
  --task-definition $TASK_DEFINITION \
  --launch-type FARGATE \
  --count 1 \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_ID],securityGroups=[$SECURITY_GROUP_ID],assignPublicIp=ENABLED}"
