import * as cdk from 'aws-cdk-lib';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

export interface TeamBatchResources {
  jobQueue: batch.JobQueue;
  jobDef: batch.EcsJobDefinition;
}

export interface BatchResources {
  alpha: TeamBatchResources;
  beta: TeamBatchResources;
}

function createTeamBatch(
  scope: Construct,
  team: string,
  vpc: ec2.IVpc,
  workerSg: ec2.ISecurityGroup,
  ecrRepoUri: string,
  taskRoleArn: string,
  execRoleArn: string,
  nlbDns: string,
  dagBucketName: string,
): TeamBatchResources {
  const computeEnv = new batch.FargateComputeEnvironment(scope, `${team}BatchCompute`, {
    computeEnvironmentName: `${team}-batch-compute`,
    vpc,
    vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    securityGroups: [workerSg],
    maxvCpus: 16,
  });

  const jobQueue = new batch.JobQueue(scope, `${team}BatchQueue`, {
    jobQueueName: `${team}-batch-queue`,
    computeEnvironments: [{ computeEnvironment: computeEnv, order: 1 }],
  });

  const logGroup = new logs.LogGroup(scope, `${team}BatchLogGroup`, {
    logGroupName: `/airflow/batch-worker/${team}`,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const jobRole = iam.Role.fromRoleArn(scope, `${team}BatchTaskRole`, taskRoleArn);
  const execRole = iam.Role.fromRoleArn(scope, `${team}BatchExecRole`, execRoleArn);

  const container = new batch.EcsFargateContainerDefinition(scope, `${team}BatchContainer`, {
    image: ecs.ContainerImage.fromRegistry(`${ecrRepoUri}:latest`),
    memory: cdk.Size.mebibytes(2048),
    cpu: 1,
    jobRole,
    executionRole: execRole,
    environment: {
      AIRFLOW__CORE__MULTI_TEAM: 'True',
      AIRFLOW__CORE__EXECUTION_API_SERVER_URL: `http://${nlbDns}:8080/execution/`,
      AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST: JSON.stringify([
        { name: 'team_alpha_dags', classpath: 'airflow.providers.amazon.aws.bundles.s3.S3DagBundle', kwargs: { bucket_name: dagBucketName, prefix: 'team_alpha' }, team_name: 'team_alpha' },
        { name: 'team_beta_dags', classpath: 'airflow.providers.amazon.aws.bundles.s3.S3DagBundle', kwargs: { bucket_name: dagBucketName, prefix: 'team_beta' }, team_name: 'team_beta' },
        { name: 'shared_dags', classpath: 'airflow.providers.amazon.aws.bundles.s3.S3DagBundle', kwargs: { bucket_name: dagBucketName, prefix: 'shared' } },
      ]),
    },
    logging: ecs.LogDrivers.awsLogs({
      streamPrefix: `batch-${team}`,
      logGroup,
    }),
  });

  const jobDef = new batch.EcsJobDefinition(scope, `${team}BatchJobDef`, {
    jobDefinitionName: `${team}-batch-job-def`,
    container,
  });

  return { jobQueue, jobDef };
}

export function createBatch(
  scope: Construct,
  vpc: ec2.IVpc,
  workerSg: ec2.ISecurityGroup,
  ecrRepoUri: string,
  taskRoleArn: string,
  execRoleArn: string,
  nlbDns: string,
  dagBucketName: string,
): BatchResources {
  const alpha = createTeamBatch(scope, 'alpha', vpc, workerSg, ecrRepoUri, taskRoleArn, execRoleArn, nlbDns, dagBucketName);
  const beta = createTeamBatch(scope, 'beta', vpc, workerSg, ecrRepoUri, taskRoleArn, execRoleArn, nlbDns, dagBucketName);

  return { alpha, beta };
}
