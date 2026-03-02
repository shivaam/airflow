#!/bin/bash
# Upload DAG files to S3. Run after changing DAGs.
# Usage: bash /opt/airflow-scripts/upload-dags.sh
set -e

DAG_BUCKET=$(aws ssm get-parameter --name /airflow-test/dag-bucket --query Parameter.Value --output text)

echo "Uploading DAGs to s3://$DAG_BUCKET/..."
aws s3 sync /tmp/dags/ s3://$DAG_BUCKET/ --delete
echo "Done. DAGs will be picked up on next dag-processor refresh cycle."
