---
inclusion: auto
---

# Airflow Local Testing Tips

## Running Unit Tests (skip DB)

When running provider unit tests locally, use these flags to avoid SQLite teardown errors:

```bash
AIRFLOW_HOME=/tmp/airflow_test .venv/bin/python -m pytest <test_file> -xvs --no-header --skip-db-tests --no-db-cleanup
```

Example:
```bash
AIRFLOW_HOME=/tmp/airflow_test .venv/bin/python -m pytest providers/amazon/tests/unit/amazon/aws/hooks/test_athena_sql.py -xvs --no-header --skip-db-tests --no-db-cleanup
```

## Running Pre-commit Checks

`prek` requires Go. Use `pre-commit` directly as a fallback:

```bash
uv run pre-commit run --files <file1> <file2>
```

Run specific hooks by name:
```bash
uv run pre-commit run ruff --files <file>
uv run pre-commit run ruff-format --files <file>
```
