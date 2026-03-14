import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { createNetwork } from './network';
import { createStorage } from './storage';
import { createIam } from './iam';
import { createNlb } from './loadbalancers';

/** Resources exposed to the compute stacks */
export interface InfraOutputs {
  vpc: ec2.Vpc;
  ec2Sg: ec2.SecurityGroup;
  workerSg: ec2.SecurityGroup;
  ec2Role: iam.Role;
  ecrRepoUri: string;
  taskRoleArn: string;
  ecsExecRoleArn: string;
  nlbArn: string;
  nlbDns: string;
}

export class InfraStack extends cdk.Stack {
  public readonly outputs: InfraOutputs;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 1. Network
    const { vpc, ec2Sg, dbSg, nlbSg, workerSg } = createNetwork(this);

    // 2. Storage
    const { db, dbSecret, logBucket, dagBucket, ecrRepo } = createStorage(this, vpc, dbSg);

    // 3. IAM
    const { ec2Role, ecsExecRole, taskRole } = createIam(this, logBucket, dagBucket, dbSecret, ecrRepo);

    // 4. NLB (no target yet - that's in compute stack)
    const nlb = createNlb(this, vpc, nlbSg);

    // 5. SSM params for infra resources (stable across compute stack updates)
    const region = cdk.Stack.of(this).region;
    const privateSubnets = vpc.privateSubnets.map((s) => s.subnetId).join(',');

    const p = (lid: string, name: string, value: string) =>
      new ssm.StringParameter(this, lid, { parameterName: name, stringValue: value });

    p('SsmDbEndpoint', '/airflow-test/db-endpoint', db.dbInstanceEndpointAddress);
    p('SsmDbPort', '/airflow-test/db-port', '5432');
    p('SsmDbSecretArn', '/airflow-test/db-secret-arn', dbSecret.secretArn);
    p('SsmDbName', '/airflow-test/db-name', 'airflow_db');
    p('SsmEcrRepo', '/airflow-test/ecr-repo', ecrRepo.repositoryUri);
    p('SsmWorkerSg', '/airflow-test/worker-sg', workerSg.securityGroupId);
    p('SsmPrivateSubnets', '/airflow-test/private-subnets', privateSubnets);
    p('SsmLogBucket', '/airflow-test/log-bucket', logBucket.bucketName);
    p('SsmDagBucket', '/airflow-test/dag-bucket', dagBucket.bucketName);
    p('SsmRegion', '/airflow-test/region', region);
    p('SsmNlbDns', '/airflow-test/nlb-dns', nlb.loadBalancerDnsName);

    new cdk.CfnOutput(this, 'NlbDns', { value: nlb.loadBalancerDnsName });

    this.outputs = {
      vpc, ec2Sg, workerSg, ec2Role,
      ecrRepoUri: ecrRepo.repositoryUri,
      taskRoleArn: taskRole.roleArn,
      ecsExecRoleArn: ecsExecRole.roleArn,
      nlbArn: nlb.loadBalancerArn,
      nlbDns: nlb.loadBalancerDnsName,
    };
  }
}
