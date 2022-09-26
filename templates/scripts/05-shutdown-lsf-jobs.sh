#!/bin/bash

# ----------------------------------------------------------------------------
# This script attempts to shut everything down cleanly by killing the
# specified cluster jobs (which should be provided in reverse launch order).
#
# NOTE: jobs are not simply killed using the job group name because
# we need to control the order in which the jobs are killed.
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
    CMD="bkill -d -J ${NAME}"
    echo "$(date) [${HOSTNAME}] running ${CMD} ..."
    ${CMD}
    sleep 10
  done

  DID_IT_WORK=$(bjobs -X -J '@{job_name_prefix}*' 2>&1 | grep -c 'No unfinished job found')

  if (( DID_IT_WORK == 1 )); then
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
bjobs -X -J '@{job_name_prefix}*'