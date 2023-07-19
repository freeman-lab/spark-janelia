#!/bin/bash

# ----------------------------------------------------------------------------
# This script attempts to shut everything down cleanly by killing the
# specified cluster jobs (which should be provided in reverse launch order).
#
# The template populated parameters are:
#   consolidate_logs: @{consolidate_logs}
#   job_name_prefix:  @{job_name_prefix}
#   run_logs_dir:     @{run_logs_dir}
# ----------------------------------------------------------------------------

DRIVER_LOG="${1}"
shift 1
REVERSE_ORDERED_JOB_IDS="$*"

if [ "@{consolidate_logs}" = "False" ]; then
  echo "$(date) [${HOSTNAME}] giving workers a chance to clean-up by sleeping for a minute ..."
  sleep 60
fi

for ATTEMPT in 1 2; do

  echo "
Attempt ${ATTEMPT} at shutting down spark jobs ...
"

  for JOB_ID in ${REVERSE_ORDERED_JOB_IDS}; do
    CMD="qstat | grep ${JOB_ID} | xargs qdel"
    echo "$(date) [${HOSTNAME}] running ${CMD} ..."
    ${CMD}
    sleep 10
  done

  # > qstat -j 6194893
  # ==============================================================
  # job_number:                 6194893
  # jclass:                     NONE
  # ...
  # start_time            1:    07/19/2023 16:04:51.868
  # job_state             1:    r
  # ...

  # TODO: verify this works
  TOTAL_RUN_COUNT=0
  for JOB_ID in ${REVERSE_ORDERED_JOB_IDS}; do
    RUN_COUNT=$(qstat -j ${JOB_ID} | grep -c "^job_state.*r")
    TOTAL_RUN_COUNT=$(( TOTAL_RUN_COUNT + RUN_COUNT ))
  done

  if (( TOTAL_RUN_COUNT == 0 )); then
    break
  fi

done

if [ -f "${DRIVER_LOG}" ]; then
  echo
  echo "last 20 driver main thread log lines from ${DRIVER_LOG}"
  echo
  grep " \[main] " "${DRIVER_LOG}" | tail -20
  echo
fi

echo "$(date) [${HOSTNAME}] removing any worker jar copies ..."
echo
rm -f "@{run_logs_dir}"/worker*/*/*/*.jar

echo "$(date) [${HOSTNAME}] done, remaining jobs are:"
qstat