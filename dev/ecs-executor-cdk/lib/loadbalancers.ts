import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as targets from 'aws-cdk-lib/aws-elasticloadbalancingv2-targets';
import { Construct } from 'constructs';

/**
 * Creates the NLB itself (infra stack). The NLB DNS is stable and gets baked
 * into ECS/Batch task definitions, so it must live in the long-lived stack.
 * Listener + target group are added separately via registerNlbTarget().
 */
export function createNlb(
  scope: Construct,
  vpc: ec2.IVpc,
  nlbSg: ec2.ISecurityGroup,
): elbv2.NetworkLoadBalancer {
  // securityGroups MUST be passed at construction time (AWS Aug 2023 constraint).
  return new elbv2.NetworkLoadBalancer(scope, 'Nlb', {
    vpc,
    internetFacing: false,
    vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
    securityGroups: [nlbSg],
  });
}

/**
 * Registers the EC2 instance as a target on the NLB (ec2 stack).
 * Uses the NLB ARN to avoid cross-stack cyclic dependency.
 */
export function registerNlbTarget(
  scope: Construct,
  vpc: ec2.IVpc,
  nlbArn: string,
  instance: ec2.Instance,
): void {
  const targetGroup = new elbv2.NetworkTargetGroup(scope, 'NlbTargetGroup', {
    vpc,
    port: 8080,
    protocol: elbv2.Protocol.TCP,
    targets: [new targets.InstanceTarget(instance, 8080)],
    healthCheck: {
      protocol: elbv2.Protocol.TCP,
      port: '8080',
    },
  });

  // Look up NLB by ARN to avoid cyclic cross-stack reference
  const nlb = elbv2.NetworkLoadBalancer.fromNetworkLoadBalancerAttributes(scope, 'NlbRef', {
    loadBalancerArn: nlbArn,
  });

  nlb.addListener('NlbListener', {
    port: 8080,
    protocol: elbv2.Protocol.TCP,
    defaultTargetGroups: [targetGroup],
  });
}
