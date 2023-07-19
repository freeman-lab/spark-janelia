#!/bin/bash

# ----------------------------------------------------------------------------
# This script polls qstat and exits successfully when enough worker jobs
# are running.  If too many polling attempts are made, the script exits
# with an error code.
# ----------------------------------------------------------------------------

set -e

WORKER_JOB_ID="${1}"
MAX_ATTEMPTS="${2}"
POLLING_INTERVAL_SECONDS="${3}"
MIN_NUMBER_OF_WORKERS="${4}"
MAX_NUMBER_OF_WORKERS="${5}"

for ATTEMPT in $(seq 1 ${MAX_ATTEMPTS}); do

  echo "$(date) [${HOSTNAME}] attempt ${ATTEMPT} checking job ${WORKER_JOB_ID}"

  # print qstat output to help with debugging later ...
  qstat -j ${WORKER_JOB_ID}

  # > qstat -j 6194893
  # ==============================================================
  # job_number:                 6194893
  # ...
  # job-array tasks:            1-1:1
  # ...
  # pending_tasks:              0
  # ...

  # TODO: someone with SGE needs to test this and make sure it works
  PENDING_TASKS=$(qstat -j ${WORKER_JOB_ID} | awk '/^pending_tasks:/ { print $2 }')
  RUN_COUNT=$(( MAX_NUMBER_OF_WORKERS - PENDING_TASKS ))

  echo "$(date) [${HOSTNAME}] found ${RUN_COUNT} running workers"

  if (( RUN_COUNT >= MIN_NUMBER_OF_WORKERS )); then
    echo "$(date) [${HOSTNAME}] exiting successfully"
    exit 0
  else
    echo "$(date) [${HOSTNAME}] sleeping ${POLLING_INTERVAL_SECONDS} seconds ..."
    sleep ${POLLING_INTERVAL_SECONDS}
  fi

done

echo "$(date) [${HOSTNAME}] not enough running workers found, exiting with error code 1"
exit 1
