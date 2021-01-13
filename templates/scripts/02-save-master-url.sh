#!/bin/bash

# ----------------------------------------------------------------------------
# This script polls the specified master log file every 30 seconds
# to look for the master URL.  Once found, the script writes the URL
# to a separate file that can be used by other scripts.
#
# The template populated parameters are:
#   master_url_path: @{master_url_path}
# ----------------------------------------------------------------------------

MASTER_LOG_FILE="$1"
MAX_ATTEMPTS="${2:-10}"

for ATTEMPT in $(seq 1 ${MAX_ATTEMPTS}); do

  echo "$(date) [${HOSTNAME}] attempt ${ATTEMPT} to find master URL in ${MASTER_LOG_FILE}"

  # 20/11/19 15:43:24 INFO Master: Starting Spark master at spark://10.36.110.41:7077
  URL_COUNT=$(grep -c "Starting Spark master at spark:" "${MASTER_LOG_FILE}")

  if (( URL_COUNT == 1 )); then
    grep "Starting Spark master at spark:" "${MASTER_LOG_FILE}" | sed 's/.*spark:/spark:/' > "@{master_url_path}"
    break
  fi

  echo "$(date) [${HOSTNAME}] URL not found, sleeping 30 seconds ..."
  sleep 30

done

if [ -f "@{master_url_path}" ]; then
  MASTER_URL=$(cat "@{master_url_path}")
  echo "$(date) [${HOSTNAME}] found URL, ${MASTER_URL} saved to @{master_url_path}"
else
  echo "$(date) [${HOSTNAME}] URL never found, exiting with error code 1"
  exit 1
fi