#!/bin/bash
# Manage Airflow services: start, stop, restart, status, logs.
#
# Usage:
#   airflow-ctl.sh start
#   airflow-ctl.sh stop
#   airflow-ctl.sh restart
#   airflow-ctl.sh status
#   airflow-ctl.sh logs                  # tail all
#   airflow-ctl.sh logs api-server       # tail one
#   airflow-ctl.sh logs scheduler
#   airflow-ctl.sh logs dag-processor
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

SERVICES=("api-server" "scheduler" "dag-processor")

_stop() {
    log_info "Stopping Airflow services..."
    pkill -9 -f "airflow api-server" 2>/dev/null || true
    pkill -9 -f "airflow api_ser" 2>/dev/null || true
    pkill -9 -f "airflow scheduler" 2>/dev/null || true
    pkill -9 -f "airflow dag-processor" 2>/dev/null || true
    pkill -9 -f "gunicorn.*airflow" 2>/dev/null || true
    fuser -k 8080/tcp 2>/dev/null || true
    sleep 3
    log_info "Stopped."
}

_start() {
    log_info "Starting Airflow services..."
    rm -f "${LOG_DIR}/api-server.log" "${LOG_DIR}/scheduler.log" "${LOG_DIR}/dag-processor.log"

    nohup airflow api-server --port 8080 > "${LOG_DIR}/api-server.log" 2>&1 &
    sleep 5
    nohup airflow scheduler > "${LOG_DIR}/scheduler.log" 2>&1 &
    nohup airflow dag-processor > "${LOG_DIR}/dag-processor.log" 2>&1 &
    sleep 2

    _status
}

_status() {
    echo ""
    echo "=== Airflow Service Status ==="
    echo ""
    for svc in "${SERVICES[@]}"; do
        if [ "$svc" = "api-server" ]; then
            # Airflow 3.x api-server runs as uvicorn, process name is "airflow api_server" (underscore)
            pid=$(pgrep -f "airflow api_ser" 2>/dev/null | head -1)
        else
            pid=$(pgrep -f "airflow ${svc}" 2>/dev/null | head -1)
        fi
        if [ -n "$pid" ]; then
            echo -e "  \033[0;32m●\033[0m ${svc}  (pid ${pid})"
        else
            echo -e "  \033[0;31m●\033[0m ${svc}  NOT RUNNING"
        fi
    done

    echo ""
    # Port check
    if ss -tlnp 2>/dev/null | grep -q ':8080'; then
        echo -e "  \033[0;32m●\033[0m Port 8080 listening"
    else
        echo -e "  \033[0;31m●\033[0m Port 8080 NOT listening"
    fi

    # DB check
    if PGPASSWORD="${DB_PASS}" psql -h "${DB_ENDPOINT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "  \033[0;32m●\033[0m DB reachable"
    else
        echo -e "  \033[0;31m●\033[0m DB NOT reachable"
    fi
    echo ""
}

_logs() {
    local svc="$1"
    if [ -z "$svc" ]; then
        # Tail all three with prefixed output
        tail -f "${LOG_DIR}/api-server.log" "${LOG_DIR}/scheduler.log" "${LOG_DIR}/dag-processor.log"
    else
        local logfile="${LOG_DIR}/${svc}.log"
        if [ -f "$logfile" ]; then
            tail -f "$logfile"
        else
            log_error "No log file: ${logfile}"
            exit 1
        fi
    fi
}

_db_reset() {
    log_warn "This will DROP and recreate the airflow_db database!"
    read -p "Are you sure? (y/N) " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        log_info "Aborted."
        return
    fi

    _stop

    log_info "Dropping and recreating database..."
    PGPASSWORD="${DB_PASS}" psql -h "${DB_ENDPOINT}" -U "${DB_USER}" -d postgres -c "DROP DATABASE IF EXISTS ${DB_NAME};"
    PGPASSWORD="${DB_PASS}" psql -h "${DB_ENDPOINT}" -U "${DB_USER}" -d postgres -c "CREATE DATABASE ${DB_NAME};"

    log_info "Running migrations..."
    airflow db migrate 2>&1 | tail -5

    log_info "Recreating teams..."
    AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_alpha 2>/dev/null || true
    AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__MULTI_TEAM=True airflow teams create team_beta 2>/dev/null || true

    log_info "DB reset complete. Run 'airflow-ctl.sh start' to restart services."
}

case "${1:-help}" in
    start)    _start ;;
    stop)     _stop ;;
    restart)  _stop; _start ;;
    status)   _status ;;
    logs)     _logs "$2" ;;
    db-reset) _db_reset ;;
    *)
        echo "Usage: $(basename "$0") {start|stop|restart|status|logs [service]|db-reset}"
        echo ""
        echo "Services: api-server, scheduler, dag-processor"
        echo "db-reset: Drop DB, recreate, migrate, recreate teams"
        exit 1
        ;;
esac
