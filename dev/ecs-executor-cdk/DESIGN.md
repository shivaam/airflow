s# Airflow ECS/Batch Executor Test Infrastructure — Design

## Goal

Provide a single `cdk deploy` that stands up a complete, self-contained AWS environment
for testing the Airflow ECS Executor and Batch Executor during development. The developer
should be able to:

1. Deploy all infrastructure with one command (`cdk deploy`)
2. SSM into the EC2 instance, run a setup script, and have Airflow running
3. Access the Airflow UI via SSM port forwarding (`localhost:8080`)
4. Switch branches/commits, rebuild, and restart with a single script
5. Tear everything down with `cdk destroy`

No manual copying of IDs, ARNs, endpoints, or passwords. Everything is wired
automatically via CDK cross-references and SSM Parameter Store.

## Why not Breeze?

In Airflow 3.x, ECS/Batch worker tasks communicate back to the API server via the
Execution API (`core.execution_api_server_url`). Workers do NOT access the metadata
DB directly. This means the API server must be network-reachable from the ECS tasks
running in AWS. A local Breeze container on your Mac is not reachable from AWS Fargate
tasks.

Therefore, the API server and scheduler must run on AWS, in the same VPC as the
ECS/Batch workers.

## Requirements

### Functional
- R1: Run Airflow API server + scheduler from any branch/commit of the airflow repo
- R2: ECS Executor: tasks launch in per-team ECS Fargate clusters (alpha, beta)
- R3: Batch Executor: tasks launch via AWS Batch job queue (Fargate compute)
- R4: Workers call back to the API server's Execution API over private networking
- R5: Switch branches without redeploying infrastructure (only rebuild + restart)
- R6: Access Airflow UI via SSM port forwarding (no public endpoints needed)
- R7: Remote logging to S3 so task logs survive container termination
- R8: Single `cdk destroy` cleans up everything
### Non-Functional
- N1: Ease of use over cost — no manual steps, no terminal babysitting
- N2: No publicly exposed endpoints — all access via SSM
- N3: Deploy time < 20 minutes
- N4: Branch switch time < 5 minutes (rebuild + restart)
- N5: No secrets in plaintext in CDK code or task definitions

## Security Model

This is a personal dev/test environment — single developer, spun up to test executor
behavior, torn down when done. The security model is calibrated for that, not for a
shared or production service.

### What is and isn't encrypted

| Path | Encryption | Rationale |
|------|-----------|-----------|
| Developer → EC2 (UI) | SSM tunnel (TLS) | SSM Session Manager encrypts the tunnel |
| Workers → Internal NLB → EC2 (Execution API) | HTTP (private VPC) | Entirely within private subnets, never leaves AWS network |
| EC2 → RDS | unencrypted | Private subnet, single-account, test data only |

There are no public-facing endpoints. The developer accesses the UI via SSM port
forwarding, which is encrypted by the SSM service. Worker traffic is HTTP within
private subnets — the traffic never leaves your VPC.

**If you later want to share this env with a team or run it longer-term**, add:
- An ALB with ACM cert for shared UI access
- RDS `force_ssl=1` + `sslmode=require` in the connection string

Those are additive changes that don't affect the rest of the design.

## Infrastructure

### Network Topology

```
VPC (10.0.0.0/16)
│
├── Public Subnets (10.0.0.0/24, 10.0.1.0/24 — AZ-a, AZ-b)
│   └── NAT Gateway × 2  ← outbound internet for private subnets
│
└── Private Subnets (10.0.2.0/24, 10.0.3.0/24 — AZ-a, AZ-b)
    ├── EC2 instance  ← API server + scheduler + dag-processor
    ├── Internal NLB  ← TCP:8080, stable DNS for Execution API
    ├── RDS PostgreSQL  ← metadata DB
    └── ECS Fargate tasks / Batch jobs
```

Nothing in the private subnets has a public IP. There are no internet-facing
resources. The developer accesses EC2 via SSM Session Manager (port forwarding
for the UI, interactive shell for administration).

### Resources Created by CDK

| Resource | Purpose |
|----------|---------|
| VPC — 2 public + 2 private subnets | Shared networking |
| NAT Gateway × 2 (one per AZ) | Outbound internet for private subnet resources |
| Internal NLB (private subnets) | Stable DNS for Execution API — workers use this URL |
| EC2 t3.large, Amazon Linux 2023 | Runs API server + scheduler + dag-processor |
| RDS PostgreSQL db.t3.medium, Multi-AZ | Airflow metadata DB, private subnet |
| ECR Repository `airflow-ecs-worker` | Worker container image |
| ECS Cluster × 2 (`alpha-cluster`, `beta-cluster`) | Per-team ECS executor targets |
| ECS Task Def × 2 (`alpha-task-def`, `beta-task-def`) | Per-team worker config |
| Batch Compute Env `airflow-batch-compute` | Fargate compute for Batch executor |
| Batch Job Queue `airflow-batch-queue` | Batch executor target |
| Batch Job Def `airflow-batch-job-def` | Worker container config for Batch |
| S3 Bucket `airflow-ecs-logs-{account}` | Remote task logging |
| SSM Parameters `/airflow-test/*` | Auto-discovery config store |
| IAM Roles (see below) | Least-privilege permissions |

Note: An internal NLB (Layer 4, TCP passthrough) is used instead of an internal ALB
for the Execution API path. The NLB DNS is stable across EC2 restarts and requires
no TLS cert management. Traffic is HTTP within the private VPC — see Security Model.

### Security Groups

```
EC2-SG:
  Inbound:  8080 from NLB-SG            ← NLB health checks and forwarded worker traffic
  Outbound: all                         ← git, pip, ECR push, AWS APIs via NAT

DB-SG:
  Inbound:  5432 from EC2-SG only    ← only the API server / scheduler hits the DB
  Outbound: (none needed)

  Workers NEVER reach the DB — Airflow 3.x workers only talk to the Execution API.
  Worker-SG is intentionally absent from DB-SG inbound rules.

NLB-SG:
  Inbound:  8080 from Worker-SG      ← only workers can reach the Execution API
  Outbound: 8080 to EC2-SG

Worker-SG (shared by alpha, beta, and Batch tasks):
  Inbound:  none                     ← workers expose no ports
  Outbound: 8080 to NLB-SG           ← Execution API callbacks (HTTP, private VPC)
            443 to 0.0.0.0/0         ← ECR pulls, S3 log writes, AWS APIs via NAT
```

Note: No inbound rule is needed on EC2-SG for SSM access. SSM Session Manager
works via the SSM agent on EC2 making outbound HTTPS calls to the SSM service
endpoint — no inbound ports required.

### Why a single Worker-SG for all teams?

Alpha, beta, and Batch workers all need identical outbound access. Using one shared
`Worker-SG` keeps the NLB-SG inbound rule simple and makes adding a third team a
one-line change. Teams are isolated by ECS cluster and task definition, not by
network policy.

### Internal NLB — solving the EC2 IP problem

The EC2 private IP changes whenever the instance is stopped and restarted, so it
can't be hardcoded into task definitions.

The internal NLB DNS name (`*.elb.amazonaws.com`) is stable for the lifetime of the
stack. CDK knows it at deploy time and writes it directly into the task definition
env vars:

```
AIRFLOW__CORE__EXECUTION_API_SERVER_URL = http://<nlb-dns>:8080/execution/
```

The NLB target group points to the EC2 instance by instance ID. When the instance
restarts, the NLB automatically re-registers it. No task definition updates needed.

### SSM Parameter Store (auto-discovery)

CDK writes all resource identifiers to SSM so the EC2 setup script can read them
without any manual input:

| Parameter | Value |
|-----------|-------|
| `/airflow-test/db-endpoint` | RDS endpoint address |
| `/airflow-test/db-port` | 5432 |
| `/airflow-test/db-secret-arn` | Secrets Manager ARN for DB credentials |
| `/airflow-test/db-name` | `airflow_db` |
| `/airflow-test/ecr-repo` | ECR repository URI |
| `/airflow-test/alpha-cluster` | `alpha-cluster` |
| `/airflow-test/alpha-task-def` | Task definition ARN |
| `/airflow-test/beta-cluster` | `beta-cluster` |
| `/airflow-test/beta-task-def` | Task definition ARN |
| `/airflow-test/batch-job-queue` | Batch job queue name |
| `/airflow-test/batch-job-def` | Batch job definition name |
| `/airflow-test/worker-sg` | Shared worker security group ID |
| `/airflow-test/private-subnets` | Comma-separated private subnet IDs |
| `/airflow-test/log-bucket` | S3 bucket name |
| `/airflow-test/region` | AWS region |
| `/airflow-test/nlb-dns` | Internal NLB DNS (Execution API base URL for task defs) |

### Communication Flow

```
Developer (cloud desktop)
  │
  │ SSM port forwarding (encrypted by SSM service)
  │ aws ssm start-session --target <instance-id> \
  │   --document-name AWS-StartPortForwardingSession \
  │   --parameters portNumber=8080,localPortNumber=8080
  │
  ▼
┌──────────────────────────────────────────────────────────────────┐
│  EC2 Instance (private subnet, no public IP)                     │
│                                                                  │
│  ┌──────────────────┐  ┌───────────────┐  ┌──────────────────┐  │
│  │   API Server     │  │   Scheduler   │  │  Dag Processor   │  │
│  │   port 8080      │  │               │  │                  │  │
│  │   - React UI     │  │  submits:     │  │  parses DAGs     │  │
│  │   - Execution API│  │  ECS tasks    │  │  from bundles    │  │
│  └────────┬─────────┘  │  Batch jobs   │  └──────────────────┘  │
│           │ SQL         └──────┬────────┘                        │
│           ▼                    │ boto3                            │
│  ┌──────────────────┐          ▼                                 │
│  │  RDS PostgreSQL  │  ┌────────────────────┐                   │
│  │  (private subnet)│  │  ECS / Batch APIs  │                   │
│  └──────────────────┘  └──────────┬─────────┘                   │
└─────────────────────────────────── │ ──────────────────────────┘
                                     │ launches containers
                          ┌──────────┴──────────┐
                          ▼                      ▼
               ┌──────────────────┐   ┌──────────────────┐
               │ ECS Fargate Task │   │   Batch Job      │
               │ (private subnet) │   │ (private subnet) │
               │                  │   │                  │
               │ EXECUTION_API_   │   │ EXECUTION_API_   │
               │ SERVER_URL =     │   │ SERVER_URL =     │
               │ http://nlb:8080/ │   │ http://nlb:8080/ │
               └────────┬─────────┘   └────────┬─────────┘
                        └──────────┬────────────┘
                                   │ HTTP :8080  (private VPC only)
                                   ▼
               Internal NLB (private subnet, NLB-SG restricts to Worker-SG)
                                   │
                                   │ TCP :8080
                                   ▼
                              EC2 :8080
```

### Security Summary

| Threat | Mitigation |
|--------|-----------|
| Public internet reaching the UI | No public endpoints — UI via SSM tunnel only |
| Public internet reaching the Execution API | Internal NLB has no public IP, not internet-facing |
| Worker reaching the DB directly | DB-SG has no rule for Worker-SG — by design |
| Plaintext Execution API traffic | HTTP within private VPC only — acceptable for test env |
| Secrets in task definitions | DB password in Secrets Manager, injected at runtime via IAM |
| EC2 accessible via SSH | No SSH key, no inbound SSH rule — SSM Session Manager only |
| S3 logs readable by anyone | Bucket policy restricts to EC2 role + worker task role only |

### EC2 Instance Details

| Property | Value |
|----------|-------|
| Instance type | t3.large (2 vCPU, 8 GB RAM) |
| AMI | Amazon Linux 2023 (latest, SSM-managed) |
| Storage | 50 GB gp3 |
| Subnet | Private — no public IP |
| SSH key | None — shell access via SSM Session Manager only |
| IAM role | SSM managed instance, ECR pull/push, Secrets Manager read, SSM Parameter read, ECS/Batch full, S3 read/write, CloudWatch Logs |

UserData (runs on first boot) installs: Python 3.12, uv, Docker, Git, AWS CLI v2, psql.

### Worker Image

The worker image must match the Airflow version on EC2 exactly — the Execution API
protocol is version-sensitive.

`rebuild-worker-image.sh` builds from the current source checkout on EC2:

```dockerfile
FROM python:3.12-slim

# Install Airflow from source (same commit as EC2)
COPY airflow-src/ /opt/airflow-src/
RUN pip install /opt/airflow-src/airflow-core \
                /opt/airflow-src/providers/amazon \
                /opt/airflow-src/task-sdk

CMD ["python", "-m", "airflow.sdk.execution_time.execute_workload"]
```

The image is tagged with the current git SHA so you can tell which version is
running in ECS/Batch from the task details.

### Scripts (on EC2 at `/opt/airflow-scripts/`)

| Script | Purpose | When to run |
|--------|---------|-------------|
| `setup-airflow.sh` | Reads SSM params, clones repo, installs Airflow, writes config, inits DB, creates teams + admin user, installs systemd units, starts services | Once after first SSM login |
| `switch-branch.sh <branch>` | Stops services, checks out branch, reinstalls Airflow, rebuilds + pushes worker image, runs `airflow db migrate`, restarts services | Each time you want to test a different branch |
| `rebuild-worker-image.sh` | Builds worker image from current checkout, pushes to ECR | When you only changed worker-side code |
| `update-config.sh` | Rewrites `/etc/airflow/airflow.env` from SSM params and restarts services | After changing executor config or adding a team |

### UI Access (SSM Port Forwarding)

```bash
# Get the instance ID from CDK outputs
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name AirflowEcsExecutorTest \
  --query "Stacks[0].Outputs[?OutputKey=='Ec2InstanceId'].OutputValue" --output text)

# Start port forwarding — Airflow UI available at http://localhost:8080
aws ssm start-session \
  --target $INSTANCE_ID \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Open `http://localhost:8080` in your browser. Login: admin / admin (change after first login).

### Shell Access (EC2)

```bash
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name AirflowEcsExecutorTest \
  --query "Stacks[0].Outputs[?OutputKey=='Ec2InstanceId'].OutputValue" --output text)

aws ssm start-session --target $INSTANCE_ID
```
