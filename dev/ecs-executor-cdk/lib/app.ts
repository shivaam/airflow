import * as cdk from 'aws-cdk-lib';
import { InfraStack } from './infra-stack';
import { Ec2Stack } from './ec2-stack';
import { EcsStack } from './ecs-stack';
import { BatchStack } from './batch-stack';

/**
 * 4-stack architecture:
 *
 *   AirflowInfra  (slow, deploy once)  — VPC, RDS, S3, ECR, NLB, IAM, SSM
 *   AirflowEc2   (medium, ~2 min)     — EC2 instance, NLB target, scripts
 *   AirflowEcs   (fast, ~30s)         — 2 ECS clusters + task defs
 *   AirflowBatch (fast, ~30s)         — Batch compute env + job queue + job def
 *
 * Deploy:  npx cdk deploy --all
 * Destroy: npx cdk destroy AirflowBatch AirflowEcs AirflowEc2 AirflowInfra --force
 *          (destroy compute stacks first, then infra)
 */

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const infra = new InfraStack(app, 'AirflowInfra', {
  terminationProtection: false,
  env,
});

new Ec2Stack(app, 'AirflowEc2', {
  terminationProtection: false,
  env,
  infra: infra.outputs,
});

new EcsStack(app, 'AirflowEcs', {
  terminationProtection: false,
  env,
  infra: infra.outputs,
});

new BatchStack(app, 'AirflowBatch', {
  terminationProtection: false,
  env,
  infra: infra.outputs,
});
