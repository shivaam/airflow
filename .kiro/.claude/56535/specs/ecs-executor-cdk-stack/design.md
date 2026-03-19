# Design — Airflow ECS/Batch Executor CDK Stack

## File Structure

```
dev/ecs-executor-cdk/
├── lib/
│   ├── app.ts          # CDK app entry point
│   ├── stack.ts        # AirflowEcsExecutorStack orchestrator
│   ├── network.ts      # VPC + security groups (4 SGs, no ALB SG)
│   ├── storage.ts      # RDS + S3 bucket + ECR repo
│   ├── iam.ts          # IAM roles (ec2, ecsExec, task)
│   ├── compute.ts      # EC2 instance + UserData
│   ├── loadbalancers.ts # Internal NLB only (no ALB)
│   ├── ecs.ts          # ECS clusters + Fargate task definitions
│   ├── batch.ts        # Batch compute env + job queue + job def
│   └── ssm.ts          # SSM parameters + CloudFormation outputs
├── cdk.json
├── package.json
└── tsconfig.json
```

Each file exports a single function called by `stack.ts` in dependency order.
`stack.ts` is the orchestrator — it wires outputs from one module into inputs of the next.

UI access is via SSM port forwarding — no ALB, no ACM cert, no domain needed.

---

## `lib/app.ts`

```typescript
import * as cdk from 'aws-cdk-lib';
import { AirflowEcsExecutorStack } from './stack';

const app = new cdk.App();
new AirflowEcsExecutorStack(app, 'AirflowEcsExecutorTest', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});
```

No env vars required — `CDK_DEFAULT_ACCOUNT` and `CDK_DEFAULT_REGION` are set automatically
by `cdk deploy` from your AWS credentials.

---

## `lib/stack.ts` — Orchestration Order

```
1. network.ts       → { vpc, ec2Sg, dbSg, nlbSg, workerSg }
2. storage.ts       → { db, dbSecret, bucket, ecrRepo }
3. iam.ts           → { ec2Role, ecsExecRole, taskRole }
4. compute.ts       → { instance }
5. loadbalancers.ts → { nlb }
6. ecs.ts           → { alphaCluster, betaCluster, alphaTaskDef, betaTaskDef }
7. batch.ts         → { jobQueue, jobDef }
8. ssm.ts           → writes all /airflow-test/* params + CfnOutputs
```

---

## `lib/network.ts`

```typescript
export interface NetworkResources {
  vpc: ec2.Vpc;
  ec2Sg: ec2.SecurityGroup;
  dbSg: ec2.SecurityGroup;
  nlbSg: ec2.SecurityGroup;
  workerSg: ec2.SecurityGroup;
}

export function createNetwork(scope: Construct): NetworkResources
```

VPC: `maxAzs: 2`, `natGateways: 2`, explicit subnet config:
- `subnetType: ec2.SubnetType.PUBLIC` — for NAT Gateways only
- `subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS` — for EC2, RDS, ECS, Batch, NLB

4 SGs (no ALB SG — UI access is via SSM tunnel):

```
ec2Sg:         inbound  TCP 8080 from nlbSg          ← worker traffic + NLB health checks
               outbound ALL      to 0.0.0.0/0        ← git, pip, ECR push, AWS APIs via NAT

dbSg:          inbound  TCP 5432 from ec2Sg          ← DB access from API server only

nlbSg:         inbound  TCP 8080 from workerSg       ← only workers reach Execution API
               outbound TCP 8080 to ec2Sg            ← forwards to EC2 + health check traffic

workerSg:      outbound TCP 8080 to nlbSg            ← Execution API callbacks
               outbound TCP 443  to 0.0.0.0/0        ← ECR pulls, S3, AWS APIs via NAT
```

No inbound rule needed for SSM — the SSM agent makes outbound HTTPS calls to the
SSM service endpoint, no inbound ports required.

---

## `lib/storage.ts`

```typescript
export interface StorageResources {
  db: rds.DatabaseInstance;
  dbSecret: secretsmanager.ISecret;
  bucket: s3.Bucket;
  ecrRepo: ecr.Repository;
}

export function createStorage(scope: Construct, vpc: ec2.IVpc, dbSg: ec2.ISecurityGroup): StorageResources
```

- S3: `removalPolicy: RemovalPolicy.DESTROY`, `autoDeleteObjects: true`, block all public access
- RDS: PostgreSQL 16, `db.t3.medium`, Multi-AZ, encrypted, `DESTROY`, no final snapshot
- ECR: `airflow-ecs-worker`, mutable tags, lifecycle keep last 5

---

## `lib/iam.ts`

```typescript
export interface IamResources {
  ec2Role: iam.Role;
  ecsExecRole: iam.Role;
  taskRole: iam.Role;
}

export function createIam(
  scope: Construct,
  bucket: s3.IBucket,
  dbSecret: secretsmanager.ISecret,
  ecrRepo: ecr.IRepository,
): IamResources
```

Three roles — unchanged from previous design. See `iam.ts` source for full policy details.

---

## `lib/compute.ts`

```typescript
export function createEc2(
  scope: Construct,
  vpc: ec2.IVpc,
  sg: ec2.ISecurityGroup,
  role: iam.IRole,
): ec2.Instance
```

Unchanged — `t3.large`, AL2023, private subnet, 50 GB gp3, no key pair, SSM only.

---

## `lib/loadbalancers.ts`

```typescript
export interface LoadBalancerResources {
  nlb: elbv2.NetworkLoadBalancer;
}

export function createLoadBalancers(
  scope: Construct,
  vpc: ec2.IVpc,
  nlbSg: ec2.ISecurityGroup,
  instance: ec2.Instance,
): LoadBalancerResources
```

Internal NLB only — no ALB, no ACM cert.

- `internetFacing: false`, private subnets, `securityGroups: [nlbSg]`
- NLB SG constraint (AWS Aug 2023): `securityGroups` MUST be passed at construction time.
- Health check flow: `nlbSg` outbound 8080 → `ec2Sg` inbound 8080 — no extra rules needed.
- TCP listener 8080 → `InstanceTarget(instance, 8080)`, health check TCP 8080

---

## `lib/ecs.ts`

Unchanged — two clusters (`alpha`, `beta`), two task defs, parameterised by team name.

---

## `lib/batch.ts`

Unchanged — `FargateComputeEnvironment`, `JobQueue`, `EcsJobDefinition`.

---

## `lib/ssm.ts`

```typescript
export interface SsmInputs {
  db: rds.DatabaseInstance;
  dbSecret: secretsmanager.ISecret;
  ecrRepo: ecr.IRepository;
  alphaCluster: ecs.Cluster;
  alphaTaskDef: ecs.FargateTaskDefinition;
  betaCluster: ecs.Cluster;
  betaTaskDef: ecs.FargateTaskDefinition;
  jobQueue: batch.JobQueue;
  jobDef: batch.EcsJobDefinition;
  workerSg: ec2.SecurityGroup;
  vpc: ec2.Vpc;
  bucket: s3.Bucket;
  nlb: elbv2.NetworkLoadBalancer;
  instance: ec2.Instance;
}

export function writeOutputs(scope: Construct, inputs: SsmInputs): void
```

Writes 16 `/airflow-test/*` SSM parameters (no `external-alb-dns` — ALB removed).

Two `CfnOutput`s:
- `Ec2InstanceId` — needed for SSM tunnel command
- `NlbDns` — for worker config verification

---

## Key CDK Import Map

```typescript
import * as cdk    from 'aws-cdk-lib';
import * as ec2    from 'aws-cdk-lib/aws-ec2';
import * as ecs    from 'aws-cdk-lib/aws-ecs';
import * as ecr    from 'aws-cdk-lib/aws-ecr';
import * as rds    from 'aws-cdk-lib/aws-rds';
import * as s3     from 'aws-cdk-lib/aws-s3';
import * as iam    from 'aws-cdk-lib/aws-iam';
import * as elbv2  from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as targets from 'aws-cdk-lib/aws-elasticloadbalancingv2-targets';
import * as ssm    from 'aws-cdk-lib/aws-ssm';
import * as logs   from 'aws-cdk-lib/aws-logs';
import * as batch  from 'aws-cdk-lib/aws-batch';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
```

All imports are from `aws-cdk-lib` — no alpha/experimental packages needed.
`acm` import removed — no ACM cert needed.
