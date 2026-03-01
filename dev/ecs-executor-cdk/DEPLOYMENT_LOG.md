# Deployment Log — Airflow ECS/Batch Executor Test Infrastructure

First successful deployment: 2026-03-01

## Issues Encountered and Resolutions

### 1. AWS SG descriptions reject non-ASCII characters

**Error:** `Character sets beyond ASCII are not supported` when creating security groups.

**Cause:** Em dashes (`—`) in SG `description` fields. AWS EC2 API only accepts ASCII.

**Fix:** Replaced all `—` with `-` in `network.ts` SG descriptions.

---

### 2. MY_IP env var returns AWS internal IP from cloud desktop

**Error:** `curl -s https://checkip.amazonaws.com` returns `52.94.133.x` (AWS internal).

**Cause:** Cloud desktop runs inside AWS network, so the public IP check returns an AWS IP, not the developer's actual IP.

**Fix:** Removed the external ALB entirely. Switched to SSM port forwarding for UI access — no public endpoints, no IP restriction needed.

---

### 3. ACM certificate requires a custom domain

**Error:** CDK hangs waiting for DNS validation during deploy.

**Cause:** ACM cert needs a domain you own + DNS CNAME record for validation.

**Fix:** Dropped ACM cert and ALB. UI access via SSM tunnel instead. No domain needed.

---

### 4. RDS takes 10-20 minutes with Multi-AZ

**Cause:** Multi-AZ provisions a standby replica in a second AZ, doubling creation time.

**Fix:** Set `multiAz: false` in `storage.ts`. Single-AZ is fine for a personal test env. Cuts RDS creation to ~5-8 min.

---

### 5. `ModuleNotFoundError: No module named 'asyncpg'`

**Error:** Airflow 3.x crashes on startup — can't find async PostgreSQL driver.

**Cause:** Airflow 3.x uses SQLAlchemy's async engine with `asyncpg` dialect for PostgreSQL. Not included in `airflow-core` deps by default.

**Fix:** `uv pip install asyncpg psycopg2-binary`

---

### 6. `airflow users create` command not found

**Error:** `invalid choice: 'users'` — the `users` subcommand doesn't exist in Airflow 3.x CLI.

**Cause:** Airflow 3.x moved user management out of the core CLI. The `users` command is either in the FAB auth manager provider or in `airflow-ctl`.

**Fix:** Switched to SimpleAuthManager with a `passwords.json` file — no extra packages needed. Config:
```ini
[core]
auth_manager = airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager

[simple_auth_manager]
passwords_file = /home/ec2-user/airflow-home/passwords.json
```

---

### 7. Wrong SimpleAuthManager import path

**Error:** `No module named 'airflow.auth'`

**Cause:** Used `airflow.auth.managers.simple...` but the actual path in Airflow 3.x is under `airflow.api_fastapi.auth.managers.simple...`.

**Fix:** Correct path: `airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager`

---

### 8. `TemplateNotFound: '/index.html'`

**Error:** API server crashes looking for `airflow/ui/dist/index.html`.

**Cause:** The React UI hasn't been built. The `dist/` directory doesn't exist until you run the frontend build.

**Fix:**
```bash
sudo dnf install -y nodejs
cd /home/ec2-user/airflow/airflow-core/src/airflow/ui
npm install --legacy-peer-deps
npm run build
```

---

### 9. `npm: command not found`

**Cause:** Node.js not installed by UserData (was not in the original plan).

**Fix:** `sudo dnf install -y nodejs`. Added to the list of manual prerequisites.

---

### 10. npm peer dependency conflict (React 19 vs @visx)

**Error:** `ERESOLVE unable to resolve dependency tree` — `@visx/group` requires React 16/17/18 but Airflow uses React 19.

**Fix:** `npm install --legacy-peer-deps`. The `@visx` charting library works fine with React 19, it just hasn't updated its peer dep declaration.

---

### 11. `pkill` not killing Airflow processes on restart

**Error:** Port 8080 still in use after running restart script.

**Cause:** Two issues:
1. Regular `pkill` sends SIGTERM — gunicorn workers can take a while to shut down, and child processes linger.
2. If processes were started by a different user (e.g. tmux as `ec2-user`, script run as `ssm-user`), `pkill` won't match.

**Fix:** Use `pkill -9` (SIGKILL) and also kill gunicorn workers explicitly:
```bash
pkill -9 -f "airflow api-server" 2>/dev/null || true
pkill -9 -f "airflow scheduler" 2>/dev/null || true
pkill -9 -f "airflow dag-processor" 2>/dev/null || true
pkill -9 -f "gunicorn.*airflow" 2>/dev/null || true
```

---

### 12. ECR repo blocks `cdk destroy` if images exist

**Cause:** ECR repos with images can't be deleted by CloudFormation unless `emptyOnDelete` is set.

**Fix:** Added `emptyOnDelete: true` to the ECR repository in `storage.ts`.

---

### 13. SSM port forwarding shows "Connection to destination port failed"

**Cause:** Nothing listening on port 8080 yet — Airflow hasn't been started.

**Fix:** Not an error. The tunnel is open and waiting. Start Airflow, and it connects automatically.

---

### 14. CloudFormation stack name mismatch after split-stack refactor

**Error:** `Stack with id AirflowCompute does not exist`

**Cause:** The old single stack `AirflowEcsExecutorTest` was still deployed. The new split stacks (`AirflowInfra` + `AirflowCompute`) hadn't been deployed yet.

**Fix:** Used the old stack name for now. Migration to split stacks is a future step.

---

### 15. Wrong ECS executor import path

**Error:** `The module/attribute could not be loaded. Current value: "::airflow.providers.amazon.executors.ecs.ecs_executor.AwsEcsExecutor"`

**Cause:** The import path was missing the `.aws.` segment. The correct path is `airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor`.

**Fix:** Updated executor config to use the full path with `.aws.`.

---

### 16. Missing per-team ECS executor config sections

**Error:** `section/key [aws_ecs_executor/cluster] not found in config`

**Cause:** The ECS executor requires a `[aws_ecs_executor]` config section with cluster, task_definition, subnets, etc. For multi-team, each team needs its own section using the `[team_name=section]` format.

**Fix:** Added `[team_alpha=aws_ecs_executor]` and `[team_beta=aws_ecs_executor]` sections to airflow.cfg with cluster, task_definition, subnets, security_groups, launch_type, and region_name.

---

### 17. LocalExecutor tasks timeout connecting to Execution API via NLB

**Error:** `httpx.ConnectTimeout: timed out` when LocalExecutor task tries to reach the Execution API.

**Cause:** `execution_api_server_url` pointed to the NLB DNS. LocalExecutor tasks run on the same EC2 instance, but the NLB security group only allows traffic from Worker-SG, not EC2-SG.

**Fix:** Changed `execution_api_server_url` to `http://localhost:8080/execution/`. ECS tasks get the NLB URL from their task definition environment variables, not from airflow.cfg.

---

### 18. JWT signature verification failed between api-server and scheduler

**Error:** `ServerResponseError: Invalid auth token: Signature verification failed`

**Cause:** When `[api_auth] jwt_secret` is not set in airflow.cfg, each Airflow process generates its own random JWT signing key in memory (`os.urandom(16)`). The api-server and scheduler end up with different keys, so tokens signed by one can't be verified by the other.

**Fix:** Added `[api_auth] jwt_secret = <generated-64-byte-key>` to airflow.cfg. The restart script auto-generates this on first run if missing.

---

### 19. Port 8080 still bound after pkill

**Error:** `[Errno 98] error while attempting to bind on address ('0.0.0.0', 8080): address already in use`

**Cause:** `pkill -9` kills the main process but gunicorn workers or other child processes may still hold the port.

**Fix:** Added `fuser -k 8080/tcp` to the restart script to kill whatever is holding the port.

---

## Current State

- **Stack deployed:** `AirflowEcsExecutorTest` (single stack, old layout)
- **EC2 instance:** `i-0e16dda07de33ddb7`
- **Airflow UI:** accessible via SSM port forwarding on `localhost:8080`
- **Auth:** SimpleAuthManager, all users are admin
- **Multi-team:** enabled, team_alpha and team_beta created
- **Executors:** LocalExecutor (global), AwsEcsExecutor (team_alpha, team_beta)
- **Services running:** api-server, scheduler, dag-processor (via nohup, restart script)
- **Scripts:** `setup-config.sh`, `create-teams.sh`, `restart-airflow.sh` in `dev/ecs-executor-cdk/scripts/`
- **Worker image:** NOT YET BUILT — ECS tasks will fail until this is done
- **LocalExecutor tasks:** working (DAGs without team assignment run successfully)

## Next Steps

### Immediate (get ECS executor testing working)

1. **Build and push worker image to ECR** (Step 9 in EC2_SETUP_NOTES.md)
2. **Create test DAGs assigned to team_alpha/team_beta**
3. **Verify end-to-end:** scheduler submits task -> ECS runs it -> worker calls Execution API via NLB -> task completes

### Short-term

4. **switch-branch.sh** — git checkout + reinstall + rebuild UI + rebuild image + db migrate + restart
5. **rebuild-worker-image.sh** — Step 9 only
6. Add nodejs and tmux to UserData
7. Systemd units for Airflow services (replace nohup)
