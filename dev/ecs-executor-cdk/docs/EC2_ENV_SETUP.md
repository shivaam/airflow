# EC2 Environment Setup — Script Reference

Scripts live in `ec2_scripts/` locally and get deployed to `/opt/airflow-scripts/` on EC2
via CDK S3 asset (downloaded by UserData on first boot). You can edit them in-place on the
host to iterate, then sync back to your repo when stable.

## How scripts get to EC2

`compute.ts` packages `ec2_scripts/` as a CDK S3 asset. UserData downloads and unpacks it:

```
cdk deploy → S3 asset upload → EC2 UserData → /opt/airflow-scripts/
```

Updating scripts locally + redeploying replaces the EC2 instance (UserData re-runs).
To update on a running instance, just edit files directly on the host.

## First-time setup

```bash
# SSM into the instance
aws ssm start-session --target <instance-id>
sudo su - ec2-user

# Run the one-shot setup (takes ~10 min: clone, install, build UI, config, DB, DAGs, start)
bash /opt/airflow-scripts/setup-airflow.sh
```

After that, the `af` command is available in every shell session.

---

## Scripts

### `env.sh`

Shared environment config sourced by every other script. Never run directly.

- Defines all paths: `AIRFLOW_HOME`, `AIRFLOW_SRC`, `AIRFLOW_VENV`, `DAG_STAGING_DIR`
- Reads SSM parameters: DB endpoint, ECR repo, S3 buckets, task defs, subnets, etc.
- Reads DB credentials from Secrets Manager
- Activates the Python venv if it exists
- Caches SSM values for the shell session (`AIRFLOW_ENV_LOADED` guard)
- Provides `log_info`, `log_warn`, `log_error`, `log_step` helper functions

### `setup-airflow.sh`

One-shot first-time setup. Run once after CDK deploy and first SSM login.

1. Clones the airflow repo from GitHub (or pulls if already cloned)
2. Creates Python 3.12 venv via `uv`, installs airflow-core, task-sdk, amazon provider
3. Builds the React UI (`npm install --legacy-peer-deps && npm run build`)
4. Writes `airflow.cfg` with multi-team config, S3 DAG bundles, ECS executor sections
5. Runs `airflow db migrate` and creates team_alpha / team_beta
6. Calls `deploy-dags.sh` to create test DAGs and upload to S3
7. Calls `airflow-ctl.sh start` to launch all services

### `airflow-ctl.sh`

Service manager with subcommands:

| Command | What it does |
|---------|-------------|
| `airflow-ctl.sh start` | Start api-server, scheduler, dag-processor (nohup) |
| `airflow-ctl.sh stop` | Kill all Airflow processes + free port 8080 |
| `airflow-ctl.sh restart` | Stop then start |
| `airflow-ctl.sh status` | Show running processes, port 8080, DB connectivity |
| `airflow-ctl.sh logs` | Tail all three service logs |
| `airflow-ctl.sh logs api-server` | Tail a specific service log |

### `deploy-dags.sh`

Creates test DAGs for each team and uploads them to S3:

- `team_alpha/alpha_dag.py` — assigned to team_alpha, runs on ECS
- `team_beta/beta_dag.py` — assigned to team_beta, runs on ECS
- `shared/shared_dag.py` — no team, runs on LocalExecutor (EC2)

Writes to `/tmp/dags/` then syncs to `s3://<dag-bucket>/` with `--delete`.
Dag-processor picks up changes on its next refresh cycle.

### `rebuild-worker-image.sh`

Builds and pushes the Airflow worker Docker image to ECR.

- Base image: `apache/airflow:latest`
- Adds: `apache-airflow-providers-amazon`, `asyncpg`, `psycopg2-binary`
- Tags as `<ecr-repo>:latest` and pushes

Run this when you need to update the worker image (e.g., after a provider change).
Not called by `setup-airflow.sh` — run separately when needed.

### `switch-branch.sh`

Switch to a different airflow branch with full rebuild. Usage:

```bash
bash /opt/airflow-scripts/switch-branch.sh my-feature-branch
```

Steps: stop services → `git checkout` → reinstall via uv → rebuild React UI →
`airflow db migrate` → rebuild + push worker image → start services.

### `airflow-cli-helpers.sh`

Auto-sourced in `.bashrc`. Provides the `af` command with tab completion:

```
af start          Start all services
af stop           Stop all services
af restart        Restart all services
af status         Show service health
af logs [svc]     Tail logs (api-server|scheduler|dag-processor)

af switch <branch>  Switch branch, rebuild, restart
af rebuild        Rebuild + push worker image to ECR
af deploy-dags    Create test DAGs + upload to S3

af db             Open psql to metadata DB
af ssm [param]    Show SSM params (or a specific one)
af config         Show airflow.cfg
af teams          List teams
af dags           List DAGs
af ecs-tasks      List running ECS tasks across both clusters
af batch-jobs     List running Batch jobs
af ecr-login      Authenticate Docker to ECR
af tunnel         Show SSM tunnel command for UI access
```

---

## Day-to-day workflow

| Task | Command |
|------|---------|
| Check if everything is running | `af status` |
| Restart after config change | `af restart` |
| Test a different branch | `af switch feature-branch` |
| Update DAGs | Edit in `/tmp/dags/`, then `af deploy-dags` |
| Rebuild worker image | `af rebuild` |
| Check ECS task status | `af ecs-tasks` |
| Read task logs in S3 | Check Airflow UI or `aws s3 ls s3://<log-bucket>/logs/` |
| Connect to metadata DB | `af db` |
| Access UI from Mac | `af tunnel` (shows the SSM command to run locally) |
