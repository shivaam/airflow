import * as cdk from 'aws-cdk-lib';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { InfraOutputs } from './infra-stack';
import { createBatch } from './batch';

export interface BatchStackProps extends cdk.StackProps {
  infra: InfraOutputs;
}

export class BatchStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: BatchStackProps) {
    super(scope, id, props);

    const { vpc, workerSg, ecrRepoUri, taskRoleArn, ecsExecRoleArn, nlbDns } = props.infra;
    const dagBucketName = `airflow-ecs-dags-${this.account}-${this.region}`;

    const { alpha, beta } = createBatch(
      this, vpc, workerSg, ecrRepoUri, taskRoleArn, ecsExecRoleArn, nlbDns, dagBucketName,
    );

    // SSM params — per-team, mirroring ECS pattern
    const p = (lid: string, name: string, value: string) =>
      new ssm.StringParameter(this, lid, { parameterName: name, stringValue: value });

    p('SsmAlphaBatchJobQueue', '/airflow-test/alpha-batch-job-queue', alpha.jobQueue.jobQueueName);
    p('SsmAlphaBatchJobDef', '/airflow-test/alpha-batch-job-def', alpha.jobDef.jobDefinitionName);
    p('SsmBetaBatchJobQueue', '/airflow-test/beta-batch-job-queue', beta.jobQueue.jobQueueName);
    p('SsmBetaBatchJobDef', '/airflow-test/beta-batch-job-def', beta.jobDef.jobDefinitionName);
  }
}
