# Tasks: AthenaSQLHook Parameter Handling Fix

## References
- GitHub Issue: https://github.com/apache/airflow/issues/55678
- File changed: `providers/amazon/src/airflow/providers/amazon/aws/hooks/athena_sql.py`
- Tests: `providers/amazon/tests/unit/amazon/aws/hooks/test_athena_sql.py`

## Task 1: Implement kwargs filtering in AthenaSQLHook.__init__
- [x] Add allowlist of `AwsGenericHook.__init__()` accepted kwargs
- [x] Filter incoming kwargs before passing to `super().__init__()`
- [x] Preserve `athena_conn_id` assignment

## Task 2: Add unit tests
- [x] `test_init_ignores_unexpected_kwargs` — verifies s3_staging_dir, work_group, driver don't crash constructor
- [x] `test_init_passes_valid_aws_kwargs` — verifies aws_conn_id, verify, region_name reach parent class
- [x] Verify all 11 existing + new tests pass with `--skip-db-tests --no-db-cleanup`

## Task 3: Validate via Breeze (pre-PR)
- [ ] Run `breeze testing providers-tests -- providers/amazon/tests/unit/amazon/aws/hooks/test_athena_sql.py -xvs`
- [ ] Confirm all tests pass in the CI-equivalent environment

## Task 4: Submit PR
- [ ] Clean up scratch file `test_athena_debug.py` from repo root
- [ ] Commit with message: `fix(amazon): Filter kwargs in AthenaSQLHook to prevent TypeError from connection extras`
- [ ] Reference `Fixes #55678` in PR description
- [ ] Create PR via `cr` tool

## Running Tests Locally

```bash
# Non-DB tests (fast, no Docker needed)
AIRFLOW_HOME=/tmp/airflow_test .venv/bin/python -m pytest \
  providers/amazon/tests/unit/amazon/aws/hooks/test_athena_sql.py \
  -xvs --skip-db-tests --no-db-cleanup

# Via Breeze (matches CI)
breeze testing providers-tests -- \
  providers/amazon/tests/unit/amazon/aws/hooks/test_athena_sql.py -xvs
```
