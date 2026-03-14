import * as cdk from 'aws-cdk-lib';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { InfraOutputs } from './infra-stack';
import { createEcs } from './ecs';

export interface EcsStackProps extends cdk.StackProps {
  infra: InfraOutputs;
}

export class EcsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EcsStackProps) {
    super(scope, id, props);

    const { vpc, ecrRepoUri, taskRoleArn, ecsExecRoleArn, nlbDns } = props.infra;
    const dagBucketName = `airflow-ecs-dags-${this.account}-${this.region}`;

    const { alphaCluster, betaCluster, alphaTaskDef, betaTaskDef } = createEcs(
      this, vpc, ecrRepoUri, taskRoleArn, ecsExecRoleArn, nlbDns, dagBucketName,
    );

    // SSM params
    const p = (lid: string, name: string, value: string) =>
      new ssm.StringParameter(this, lid, { parameterName: name, stringValue: value });

    p('SsmAlphaCluster', '/airflow-test/alpha-cluster', alphaCluster.clusterName);
    p('SsmAlphaTaskDef', '/airflow-test/alpha-task-def', alphaTaskDef.family);
    p('SsmBetaCluster', '/airflow-test/beta-cluster', betaCluster.clusterName);
    p('SsmBetaTaskDef', '/airflow-test/beta-task-def', betaTaskDef.family);
  }
}
