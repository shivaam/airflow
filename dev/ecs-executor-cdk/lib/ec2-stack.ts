import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { InfraOutputs } from './infra-stack';
import { createEc2 } from './compute';
import { registerNlbTarget } from './loadbalancers';

export interface Ec2StackProps extends cdk.StackProps {
  infra: InfraOutputs;
}

export class Ec2Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: Ec2StackProps) {
    super(scope, id, props);

    const { vpc, ec2Sg, ec2Role, nlbArn } = props.infra;

    // 1. EC2 instance
    const instance = createEc2(this, vpc, ec2Sg, ec2Role);

    // 2. Register EC2 as NLB target (listener + target group)
    registerNlbTarget(this, vpc, nlbArn, instance);

    // Output for SSM tunnel
    new cdk.CfnOutput(this, 'Ec2InstanceId', {
      value: instance.instanceId,
      description: 'SSM tunnel: aws ssm start-session --target <id> --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8080',
    });
  }
}
