#!/bin/bash
# Restart all Airflow services on EC2.
# Usage: bash /opt/airflow-scripts/restart-airflow.sh
set -e

source /home/ec2-user/airflow-venv/bin/activate
export AIRFLOW_HOME=/home/ec2-user/airflow-home

echo "Stopping any running Airflow processes..."
pkill -9 -f "airflow api-server" 2>/dev/null || true
pkill -9 -f "airflow scheduler" 2>/dev/null || true
pkill -9 -f "airflow dag-processor" 2>/dev/null || true
pkill -9 -f "gunicorn.*airflow" 2>/dev/null || true
fuser -k 8080/tcp 2>/dev/null || true
sleep 5

echo "Cleaning old logs..."
rm -f /tmp/api-server.log /tmp/scheduler.log /tmp/dag-processor.log
rm -rf $AIRFLOW_HOME/logs/*

# Ensure a shared JWT secret exists in config (one-time)
if ! grep -q '\[api_auth\]' $AIRFLOW_HOME/airflow.cfg 2>/dev/null; then
    JWT_SECRET=$(python -c "import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(64)).decode())")
    cat >> $AIRFLOW_HOME/airflow.cfg << EOF

[api_auth]
jwt_secret = ${JWT_SECRET}
EOF
    echo "Generated and saved JWT secret to airflow.cfg"
fi

echo "Starting api-server..."
nohup airflow api-server --port 8080 > /tmp/api-server.log 2>&1 &
sleep 5

echo "Starting scheduler..."
nohup airflow scheduler > /tmp/scheduler.log 2>&1 &

echo "Starting dag-processor..."
nohup airflow dag-processor > /tmp/dag-processor.log 2>&1 &

sleep 2
echo ""
echo "All services started. PIDs:"
pgrep -a -f "airflow api-server" || echo "  api-server: NOT RUNNING"
pgrep -a -f "airflow scheduler" || echo "  scheduler: NOT RUNNING"
pgrep -a -f "airflow dag-processor" || echo "  dag-processor: NOT RUNNING"
echo ""
echo "Logs:"
echo "  tail -f /tmp/api-server.log"
echo "  tail -f /tmp/scheduler.log"
echo "  tail -f /tmp/dag-processor.log"
