import * as cdk from 'aws-cdk-lib';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

export interface IamResources {
  ec2Role: iam.Role;
  ecsExecRole: iam.Role;
  taskRole: iam.Role;
}

export function createIam(
  scope: Construct,
  logBucket: s3.IBucket,
  dagBucket: s3.IBucket,
  dbSecret: secretsmanager.ISecret,
  ecrRepo: ecr.IRepository,
): IamResources {
  // --- EC2 instance role ---
  const ec2Role = new iam.Role(scope, 'Ec2Role', {
    assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
    managedPolicies: [
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
    ],
  });

  // ECR push/pull
  ec2Role.addToPolicy(
    new iam.PolicyStatement({
      actions: [
        'ecr:GetAuthorizationToken',
        'ecr:BatchGetImage',
        'ecr:GetDownloadUrlForLayer',
        'ecr:BatchCheckLayerAvailability',
        'ecr:InitiateLayerUpload',
        'ecr:UploadLayerPart',
        'ecr:CompleteLayerUpload',
        'ecr:PutImage',
      ],
      resources: [ecrRepo.repositoryArn, '*'], // GetAuthorizationToken requires *
    }),
  );

  // Secrets Manager — DB password only
  ec2Role.addToPolicy(
    new iam.PolicyStatement({
      actions: ['secretsmanager:GetSecretValue'],
      resources: [dbSecret.secretArn],
    }),
  );

  // SSM Parameter Store — /airflow-test/* only
  ec2Role.addToPolicy(
    new iam.PolicyStatement({
      actions: ['ssm:GetParameter', 'ssm:GetParameters'],
      resources: [
        `arn:aws:ssm:${cdk.Stack.of(scope).region}:${cdk.Stack.of(scope).account}:parameter/airflow-test/*`,
      ],
    }),
  );

  // ECS — submit and manage tasks
  ec2Role.addToPolicy(
    new iam.PolicyStatement({
      actions: ['ecs:RunTask', 'ecs:DescribeTasks', 'ecs:StopTask', 'ecs:ListTasks'],
      resources: ['*'],
    }),
  );

  // Batch — submit and manage jobs
  ec2Role.addToPolicy(
    new iam.PolicyStatement({
      actions: [
        'batch:SubmitJob',
        'batch:DescribeJobs',
        'batch:TerminateJob',
        'batch:ListJobs',
      ],
      resources: ['*'],
    }),
  );

  // S3 buckets
  logBucket.grantReadWrite(ec2Role);
  dagBucket.grantReadWrite(ec2Role);

  // CloudWatch Logs
  ec2Role.addToPolicy(
    new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: ['*'],
    }),
  );

  // IAM PassRole — needed to pass task/job roles when submitting ECS tasks and Batch jobs
  ec2Role.addToPolicy(
    new iam.PolicyStatement({
      actions: ['iam:PassRole'],
      resources: ['*'],
      conditions: {
        StringLike: {
          'iam:PassedToService': ['ecs-tasks.amazonaws.com', 'batch.amazonaws.com'],
        },
      },
    }),
  );

  // --- ECS task execution role (used by ECS agent to pull image + write logs) ---
  const ecsExecRole = new iam.Role(scope, 'EcsExecRole', {
    assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    managedPolicies: [
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonECSTaskExecutionRolePolicy'),
    ],
  });

  // --- ECS task role / Batch job role (used by the running worker container) ---
  const taskRole = new iam.Role(scope, 'TaskRole', {
    assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
  });

  logBucket.grantReadWrite(taskRole);
  dagBucket.grantRead(taskRole);

  taskRole.addToPolicy(
    new iam.PolicyStatement({
      actions: ['ssm:GetParameter'],
      resources: [
        `arn:aws:ssm:${cdk.Stack.of(scope).region}:${cdk.Stack.of(scope).account}:parameter/airflow-test/*`,
      ],
    }),
  );

  taskRole.addToPolicy(
    new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: ['*'],
    }),
  );

  return { ec2Role, ecsExecRole, taskRole };
}
