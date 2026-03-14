import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

export interface NetworkResources {
  vpc: ec2.Vpc;
  ec2Sg: ec2.SecurityGroup;
  dbSg: ec2.SecurityGroup;
  nlbSg: ec2.SecurityGroup;
  workerSg: ec2.SecurityGroup;
}

export function createNetwork(scope: Construct): NetworkResources {
  const vpc = new ec2.Vpc(scope, 'Vpc', {
    ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
    maxAzs: 2,
    natGateways: 2,
    subnetConfiguration: [
      {
        name: 'public',
        subnetType: ec2.SubnetType.PUBLIC,
        cidrMask: 24,
      },
      {
        name: 'private',
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        cidrMask: 24,
      },
    ],
  });

  // EC2 — API server, scheduler, dag-processor
  // allowAllOutbound: true — needs ECR push, pip, git, AWS APIs via NAT
  const ec2Sg = new ec2.SecurityGroup(scope, 'Ec2Sg', {
    vpc,
    description: 'EC2 - API server, scheduler, dag-processor',
    allowAllOutbound: true,
  });

  // RDS — EC2 only, no workers (Airflow 3.x workers never touch DB directly)
  const dbSg = new ec2.SecurityGroup(scope, 'DbSg', {
    vpc,
    description: 'RDS PostgreSQL - EC2 only',
    allowAllOutbound: false,
  });

  // NLB SG must be passed at construction time — cannot be added after (AWS Aug 2023)
  const nlbSg = new ec2.SecurityGroup(scope, 'NlbSg', {
    vpc,
    description: 'Internal NLB - Execution API, workers only',
    allowAllOutbound: false,
  });

  // Shared across alpha, beta, Batch — teams isolated by cluster/task-def
  const workerSg = new ec2.SecurityGroup(scope, 'WorkerSg', {
    vpc,
    description: 'ECS/Batch workers - shared across alpha, beta, Batch',
    allowAllOutbound: false,
  });

  // ec2Sg — SSM tunneling uses SSM endpoints (HTTPS via NAT), no inbound rule needed for UI
  ec2Sg.addIngressRule(nlbSg, ec2.Port.tcp(8080), 'Worker Execution API + NLB health checks');

  // dbSg
  dbSg.addIngressRule(ec2Sg, ec2.Port.tcp(5432), 'DB access from EC2 only');

  // nlbSg — health probes flow nlbSg outbound → ec2Sg inbound, no extra rules needed
  nlbSg.addIngressRule(workerSg, ec2.Port.tcp(8080), 'Workers reach Execution API via NLB');
  nlbSg.addEgressRule(ec2Sg, ec2.Port.tcp(8080), 'Forward to EC2 + health check traffic');

  // workerSg
  workerSg.addEgressRule(nlbSg, ec2.Port.tcp(8080), 'Execution API callbacks');
  workerSg.addEgressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443), 'ECR, S3, AWS APIs via NAT');

  return { vpc, ec2Sg, dbSg, nlbSg, workerSg };
}
