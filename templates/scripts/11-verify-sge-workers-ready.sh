#!/bin/bash

# ----------------------------------------------------------------------------
# This script polls qstat and exits successfully when enough worker jobs
# are running.  If too many polling attempts are made, the script exits
# with an error code.
# ----------------------------------------------------------------------------

set -e

WORKER_JOB_NAME="${1}"
MAX_ATTEMPTS="${2}"
POLLING_INTERVAL_SECONDS="${3}"
MIN_NUMBER_OF_WORKERS="${4}"

for ATTEMPT in $(seq 1 ${MAX_ATTEMPTS}); do

  echo "$(date) [${HOSTNAME}] attempt ${ATTEMPT} checking job ${WORKER_JOB_NAME}"

  # print qstat output to help with debugging later ...
  qstat -j ${WORKER_JOB_NAME}

  # Job ID      Username  Queue         Jobname     Limit  State  Elapsed
  #------      --------  -----         -------     -----  -----  -------
  #240         hmprof    sun-long      STDIN       27:00  Hold   --
  #379         jqpublic  sun-long      foo         27:00  Run    00:03

  # TODO: someone with SGE needs to test this and make sure it works
  # TODO: need to ensure "Run" is coming from State column
  RUN_COUNT=$(qstat -j ${WORKER_JOB_NAME} | grep -c 'Run')

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
