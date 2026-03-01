# Tasks — Airflow ECS/Batch Executor CDK Stack

## Task 1 — package.json
- [x] Add `ts-node` to `devDependencies`

## Task 2 — `lib/app.ts`
- [x] Instantiate `AirflowEcsExecutorStack` with `env` props (no env vars required)

## Task 3 — `lib/network.ts`
- [x] VPC: `10.0.0.0/16`, `maxAzs: 2`, `natGateways: 2`, PUBLIC + PRIVATE_WITH_EGRESS subnets
- [x] Create 4 SGs: `ec2Sg`, `dbSg`, `nlbSg`, `workerSg` (no ALB SG — UI via SSM tunnel)
- [x] Add rules: ec2Sg ← nlbSg:8080, dbSg ← ec2Sg:5432, nlbSg ← workerSg:8080, workerSg → nlbSg:8080 + 0.0.0.0/0:443
- [x] Export `NetworkResources` interface + `createNetwork(scope)` function

## Task 4 — `lib/storage.ts`
- [x] S3 bucket, RDS PostgreSQL 16, ECR repo — all with `DESTROY` removal policy
- [x] Export `StorageResources` interface + `createStorage(scope, vpc, dbSg)` function

## Task 5 — `lib/iam.ts`
- [x] `ec2Role`, `ecsExecRole`, `taskRole` with least-privilege policies
- [x] Export `IamResources` interface + `createIam(scope, bucket, dbSecret, ecrRepo)` function

## Task 6 — `lib/compute.ts`
- [x] EC2: `t3.large`, AL2023, private subnet, 50 GB gp3, no key pair, SSM only
- [x] UserData: Python 3.12, uv, Docker, Git, AWS CLI v2, psql
- [x] Export `createEc2(scope, vpc, sg, role)` function

## Task 7 — `lib/loadbalancers.ts`
- [x] Internal NLB only: `internetFacing: false`, `securityGroups: [nlbSg]` at construction time
- [x] TCP listener 8080 → `InstanceTarget(instance, 8080)`, health check TCP 8080
- [x] Export `LoadBalancerResources` interface (`nlb` only) + `createLoadBalancers(scope, vpc, nlbSg, instance)`

## Task 8 — `lib/ecs.ts`
- [x] Two clusters + two task defs, parameterised by team name
- [x] Env var: `AIRFLOW__CORE__EXECUTION_API_SERVER_URL = http://${nlbDns}:8080/execution/`
- [x] Export `EcsResources` interface + `createEcs(...)` function

## Task 9 — `lib/batch.ts`
- [x] `FargateComputeEnvironment`, `JobQueue`, `EcsJobDefinition` with same env var
- [x] Export `BatchResources` interface + `createBatch(...)` function

## Task 10 — `lib/ssm.ts`
- [x] 16 SSM parameters under `/airflow-test/*`
- [x] Two `CfnOutput`s: `Ec2InstanceId`, `NlbDns`
- [x] Export `writeOutputs(scope, inputs: SsmInputs)` function

## Task 11 — `lib/stack.ts`
- [x] Orchestrate modules in dependency order: network → storage → iam → compute → loadbalancers → ecs → batch → ssm
- [x] No custom stack props needed (no myIp, no domainName)
