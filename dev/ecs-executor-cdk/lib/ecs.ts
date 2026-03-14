import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface EcsResources {
  alphaCluster: ecs.Cluster;
  betaCluster: ecs.Cluster;
  alphaTaskDef: ecs.FargateTaskDefinition;
  betaTaskDef: ecs.FargateTaskDefinition;
}

function createTeamResources(
  scope: Construct,
  team: string,
  vpc: ec2.IVpc,
  ecrRepoUri: string,
  taskRoleArn: string,
  execRoleArn: string,
  nlbDns: string,
  dagBucketName: string,
): { cluster: ecs.Cluster; taskDef: ecs.FargateTaskDefinition } {
  const cluster = new ecs.Cluster(scope, `${team}Cluster`, {
    vpc,
    clusterName: `${team}-cluster`,
  });

  // Import roles by ARN to avoid cross-stack construct references
  const taskRole = iam.Role.fromRoleArn(scope, `${team}TaskRole`, taskRoleArn);
  const execRole = iam.Role.fromRoleArn(scope, `${team}ExecRole`, execRoleArn);

  const taskDef = new ecs.FargateTaskDefinition(scope, `${team}TaskDef`, {
    family: `${team}-task-def`,
    cpu: 1024,
    memoryLimitMiB: 2048,
    taskRole,
    executionRole: execRole,
  });

  const logGroup = new logs.LogGroup(scope, `${team}LogGroup`, {
    logGroupName: `/airflow/ecs-worker/${team}`,
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  taskDef.addContainer(`${team}Container`, {
    containerName: 'airflow-worker',
    image: ecs.ContainerImage.fromRegistry(`${ecrRepoUri}:latest`),
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
      streamPrefix: team,
      logGroup,
    }),
  });

  return { cluster, taskDef };
}

export function createEcs(
  scope: Construct,
  vpc: ec2.IVpc,
  ecrRepoUri: string,
  taskRoleArn: string,
  execRoleArn: string,
  nlbDns: string,
  dagBucketName: string,
): EcsResources {
  const alpha = createTeamResources(scope, 'alpha', vpc, ecrRepoUri, taskRoleArn, execRoleArn, nlbDns, dagBucketName);
  const beta = createTeamResources(scope, 'beta', vpc, ecrRepoUri, taskRoleArn, execRoleArn, nlbDns, dagBucketName);

  return {
    alphaCluster: alpha.cluster,
    betaCluster: beta.cluster,
    alphaTaskDef: alpha.taskDef,
    betaTaskDef: beta.taskDef,
  };
}
