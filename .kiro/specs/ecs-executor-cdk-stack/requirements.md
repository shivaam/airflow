# Requirements — Airflow ECS/Batch Executor CDK Stack

## Reference
#[[file:dev/ecs-executor-cdk/DESIGN.md]]

## Overview

Implement a TypeScript CDK stack in `dev/ecs-executor-cdk/` that deploys all infrastructure
described in the DESIGN.md. A single `cdk deploy` should produce a fully wired environment
where the developer can SSM into EC2, run a setup script, and have Airflow running with
ECS and Batch executors. UI access is via SSM port forwarding — no public endpoints needed.

## Requirements

### R1 — Entry point
- `lib/app.ts` instantiates `AirflowEcsExecutorStack` with `env` props
- No env vars required (CDK_DEFAULT_ACCOUNT/REGION set automatically)
- Stack name: `AirflowEcsExecutorTest`

### R2 — VPC
- CIDR `10.0.0.0/16`
- 2 public subnets (AZ-a, AZ-b) — for NAT Gateways only
- 2 private subnets (AZ-a, AZ-b) — for EC2, RDS, ECS tasks, Batch jobs, Internal NLB
- NAT Gateway × 2 (one per AZ) for private subnet outbound internet

### R3 — Security Groups (4 SGs — no ALB SG)
- `ec2Sg` — inbound 8080 from `nlbSg`, outbound all
- `dbSg` — inbound 5432 from `ec2Sg` only
- `nlbSg` — inbound 8080 from `workerSg`, outbound 8080 to `ec2Sg`
- `workerSg` — no inbound, outbound 8080 to `nlbSg` and 443 to `0.0.0.0/0`

### R4 — Internal NLB
- Internal (not internet-facing), private subnets
- TCP listener on port 8080
- Security group: `nlbSg` (must be passed at construction time)
- Target: EC2 instance by instance ID on port 8080
- Used by workers as `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`

### R5 — EC2 Instance
- Type: `t3.large`
- AMI: latest Amazon Linux 2023 (SSM-managed, `AmazonLinux2023`)
- Subnet: private (no public IP)
- Storage: 50 GB gp3 root volume
- No SSH key pair — access via SSM Session Manager only
- IAM instance profile with permissions:
  - SSM managed instance core (`AmazonSSMManagedInstanceCore`)
  - ECR: push/pull operations
  - Secrets Manager: `GetSecretValue` (scoped to DB secret ARN)
  - SSM Parameter Store: `GetParameter`, `GetParameters` on `/airflow-test/*`
  - ECS: `RunTask`, `DescribeTasks`, `StopTask`, `ListTasks` on all resources
  - Batch: `SubmitJob`, `DescribeJobs`, `TerminateJob`, `ListJobs` on all resources
  - S3: read/write on log bucket
  - CloudWatch Logs: write
  - IAM PassRole: to ECS and Batch services
- UserData: installs Python 3.12, uv, Docker, Git, AWS CLI v2, psql

### R6 — RDS PostgreSQL
- Engine: PostgreSQL 16
- Instance class: `db.t3.medium`
- Multi-AZ: true
- Storage: 20 GB, encrypted
- DB name: `airflow_db`
- Subnet group: private subnets
- Security group: `dbSg`
- Credentials: auto-generated, stored in Secrets Manager
- Deletion protection: false (test env, easy teardown)
- Skip final snapshot: true

### R7 — ECR Repository
- Name: `airflow-ecs-worker`
- Image tag mutability: mutable (so `latest` can be overwritten)
- Lifecycle rule: keep last 5 images

### R8 — ECS Clusters and Task Definitions
- Two clusters: `alpha-cluster`, `beta-cluster`
- Two task definitions: `alpha-task-def`, `beta-task-def`
- Both task defs:
  - Launch type: Fargate, CPU: 1024, Memory: 2048
  - Execution role: ECS task execution role (ECR pull + CloudWatch Logs)
  - Task role: S3 log write, SSM read, CloudWatch Logs
  - Container image: ECR repo URI + `:latest`
  - Environment variable: `AIRFLOW__CORE__EXECUTION_API_SERVER_URL = http://<nlb-dns>:8080/execution/`
  - Log driver: `awslogs`, stream prefix `alpha` or `beta`

### R9 — AWS Batch
- Compute environment: Managed Fargate, max 16 vCPUs, private subnets, `workerSg`
- Job queue: linked to compute env, priority 1
- Job definition: ECR `:latest`, 1 vCPU / 2048 MB, same task role, same env var

### R10 — S3 Log Bucket
- Name: `airflow-ecs-logs-{account}-{region}` (unique, no public access)
- Versioning: off (test env)
- Auto-delete objects on `cdk destroy`: true

### R11 — SSM Parameters
Write 16 parameters to `/airflow-test/*` (see DESIGN.md SSM table).

### R12 — CloudFormation Outputs
- `Ec2InstanceId` — EC2 instance ID (for SSM tunnel command)
- `NlbDns` — Internal NLB DNS name

### R13 — package.json
Add `ts-node` to `devDependencies` (required by `cdk.json` app command).
