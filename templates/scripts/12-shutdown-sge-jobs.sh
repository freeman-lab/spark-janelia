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
REVERSE_ORDERED_JOB_NAMES="$*"

if [ "@{consolidate_logs}" = "False" ]; then
  echo "$(date) [${HOSTNAME}] giving workers a chance to clean-up by sleeping for a minute ..."
  sleep 60
fi

for ATTEMPT in 1 2; do

  for NAME in ${REVERSE_ORDERED_JOB_NAMES}; do
    CMD="qstat -xml | grep -B 7 '<JB_name>${NAME}</JB_name>' | grep '<JB_job_number>' | awk -F'[<>]' '{print \$3}' | xargs qdel"
    echo "$(date) [${HOSTNAME}] running ${CMD} ..."
    ${CMD}
    sleep 10
  done

  # TODO: verify this works
  RUN_COUNT=$(qstat -j '@{job_name_prefix}*' 2>&1 | grep -c 'Run')

  if (( RUN_COUNT == 0 )); then
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
qstat -j '@{job_name_prefix}*'