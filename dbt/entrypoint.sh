#!/bin/bash
set -e

# Listing all files and directories in /usr/src/app for debugging
echo "Listing all files and directories in /usr/src/app:"
find ./ /usr/src/app

# Optionally, explicitly check for the presence of key dbt files
echo "Checking for essential dbt files:"
[[ -f /usr/src/app/dbt_project.yml ]] && echo "Found dbt_project.yml" || echo "dbt_project.yml is missing"
[[ -f /usr/src/app/profiles.yml ]] && echo "Found profiles.yml" || echo "profiles.yml is missing"


# Set environment variables for the INT environment
export DBT_TARGET_ENVIRONMENT=int
export GCP_PROJECT_ID=dbt-analytics-engineer-403622
export DBT_INT_DATASET=dbt_stage
#export GOOGLE_APPLICATION_CREDENTIALS=/path/to/int/service/account/key.json

# Run dbt models
dbt run --profiles-dir /usr/src/app --target $DBT_TARGET_ENVIRONMENT > /usr/src/app/dbt_run_log.txt 2>&1
# Optionally, display the log file for debugging
cat /usr/src/app/dbt_run_log.txt





# Keep the container running (if necessary)
tail -f /dev/null
