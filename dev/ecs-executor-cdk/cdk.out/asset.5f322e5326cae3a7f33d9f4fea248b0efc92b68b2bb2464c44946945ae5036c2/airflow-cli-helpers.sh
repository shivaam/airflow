#!/bin/bash
# Source this in .bashrc for af-* shortcuts on EC2.
# Added automatically by setup-airflow.sh.

export AIRFLOW_HOME="/home/ec2-user/airflow-home"
export SCRIPTS_DIR="/opt/airflow-scripts"

# Activate venv if available
if [ -f "/home/ec2-user/airflow-venv/bin/activate" ]; then
    source "/home/ec2-user/airflow-venv/bin/activate"
fi

# ── af command — unified entry point ───────────────────────────────────
af() {
    case "${1:-help}" in
        start)      bash "${SCRIPTS_DIR}/airflow-ctl.sh" start ;;
        stop)       bash "${SCRIPTS_DIR}/airflow-ctl.sh" stop ;;
        restart)    bash "${SCRIPTS_DIR}/airflow-ctl.sh" restart ;;
        status)     bash "${SCRIPTS_DIR}/airflow-ctl.sh" status ;;
        logs)       bash "${SCRIPTS_DIR}/airflow-ctl.sh" logs "$2" ;;
        deploy-dags) bash "${SCRIPTS_DIR}/deploy-dags.sh" ;;
        rebuild)    bash "${SCRIPTS_DIR}/rebuild-worker-image.sh" ;;
        switch)     bash "${SCRIPTS_DIR}/switch-branch.sh" "$2" ;;
        db)
            source "${SCRIPTS_DIR}/env.sh"
            PGPASSWORD="${DB_PASS}" psql -h "${DB_ENDPOINT}" -U "${DB_USER}" -d "${DB_NAME}"
            ;;
        ssm)
            if [ -z "$2" ]; then
                echo "All /airflow-test/ params:"
                aws ssm get-parameters-by-path --path /airflow-test/ --query "Parameters[].{Name:Name,Value:Value}" --output table
            else
                aws ssm get-parameter --name "/airflow-test/$2" --query Parameter.Value --output text
            fi
            ;;
        ecr-login)
            source "${SCRIPTS_DIR}/env.sh"
            aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_REPO}"
            echo "Logged into ECR."
            ;;
        db-reset)   bash "${SCRIPTS_DIR}/airflow-ctl.sh" db-reset ;;
        ecs-tasks)
            source "${SCRIPTS_DIR}/env.sh"
            echo "=== alpha-cluster ==="
            aws ecs list-tasks --cluster alpha-cluster --region "${REGION}" --output table 2>/dev/null || echo "  (none)"
            echo "=== beta-cluster ==="
            aws ecs list-tasks --cluster beta-cluster --region "${REGION}" --output table 2>/dev/null || echo "  (none)"
            ;;
        batch-jobs)
            source "${SCRIPTS_DIR}/env.sh"
            echo "=== Batch jobs (RUNNING) ==="
            aws batch list-jobs --job-queue airflow-batch-queue --job-status RUNNING --region "${REGION}" --output table 2>/dev/null || echo "  (none)"
            ;;
        config)
            cat "${AIRFLOW_HOME}/airflow.cfg"
            ;;
        teams)
            AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams list
            ;;
        dags)
            airflow dags list 2>/dev/null | head -30
            ;;
        tunnel)
            INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
            echo "Run this on your Mac:"
            echo ""
            echo "  aws ssm start-session --target ${INSTANCE_ID} \\"
            echo "    --document-name AWS-StartPortForwardingSession \\"
            echo "    --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'"
            echo ""
            echo "Then open http://localhost:8080"
            ;;
        *)
            echo "af - Airflow helper commands"
            echo ""
            echo "Services:"
            echo "  af start          Start all services"
            echo "  af stop           Stop all services"
            echo "  af restart        Restart all services"
            echo "  af status         Show service health"
            echo "  af logs [svc]     Tail logs (api-server|scheduler|dag-processor)"
            echo ""
            echo "Development:"
            echo "  af switch <branch>  Switch branch, rebuild, restart"
            echo "  af rebuild        Rebuild + push worker image to ECR"
            echo "  af deploy-dags    Create test DAGs + upload to S3"
            echo ""
            echo "Inspection:"
            echo "  af db             Open psql to metadata DB"
            echo "  af ssm [param]    Show SSM params (or specific one)"
            echo "  af config         Show airflow.cfg"
            echo "  af teams          List teams"
            echo "  af dags           List DAGs"
            echo "  af ecs-tasks      List running ECS tasks"
            echo "  af batch-jobs     List running Batch jobs"
            echo "  af ecr-login      Authenticate Docker to ECR"
            echo "  af tunnel         Show SSM tunnel command for UI"
            ;;
    esac
}

# Tab completion for af
_af_completions() {
    local cmds="start stop restart status logs deploy-dags rebuild switch db db-reset ssm config teams dags ecs-tasks batch-jobs ecr-login tunnel help"
    COMPREPLY=($(compgen -W "$cmds" -- "${COMP_WORDS[1]}"))
}
complete -F _af_completions af
