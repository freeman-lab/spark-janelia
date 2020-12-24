#!/bin/bash

# ----------------------------------------------------------------------------
# This script submits all jobs to the LSF cluster scheduler.
#
# The template populated parameters are:
#   common_job_args:      @{common_job_args}
#   driver_slots:         @{driver_slots}
#   job_name_prefix:      @{job_name_prefix}
#   job_runtime_limit:    @{job_runtime_limit}
#   max_worker_nodes:     @{max_worker_nodes}
#   min_worker_nodes:     @{min_worker_nodes}
#   run_logs_dir:         @{run_logs_dir}
#   run_scripts_dir:      @{run_scripts_dir}
#   worker_slots:         @{worker_slots}
# ----------------------------------------------------------------------------

set -e

# use shell group to tee all output to log file
{

MASTER_JOB_NAME="@{job_name_prefix}_ma"
URL_JOB_NAME="@{job_name_prefix}_ur"
WORKER_JOB_NAME="@{job_name_prefix}_wo"
DRIVER_JOB_NAME="@{job_name_prefix}_dr"
SHUTDOWN_JOB_NAME="@{job_name_prefix}_sd"

MASTER_LOG="@{run_logs_dir}/01-master.log"
DRIVER_LOG="@{run_logs_dir}/04-driver.log"

REVERSE_ORDERED_JOB_NAMES="${DRIVER_JOB_NAME} ${WORKER_JOB_NAME} ${URL_JOB_NAME} ${MASTER_JOB_NAME}"

echo "$(date) [${HOSTNAME}] submitting jobs to scheduler"

bsub @{common_job_args} -W @{job_runtime_limit} \
  -J ${MASTER_JOB_NAME} -n1 \
  -o "${MASTER_LOG}" \
  "@{run_scripts_dir}/01-launch-master.sh"

bsub @{common_job_args} -W 0:10 \
  -J ${URL_JOB_NAME} -w "started(${MASTER_JOB_NAME})" -ti -n1 \
  -o "@{run_logs_dir}/02-url.log" \
  "@{run_scripts_dir}/02-save-master-url.sh" "${MASTER_LOG}"

# include job index in worker log name so that output from multiple workers are separated
bsub @{common_job_args} -W @{job_runtime_limit} \
  -J ${WORKER_JOB_NAME}[1-@{max_worker_nodes}] -w "done(${URL_JOB_NAME})" -ti -n@{worker_slots} \
  -o "@{run_logs_dir}/worker-%I-launch.log" \
  "@{run_scripts_dir}/03-launch-worker.sh"

# dependency that waits for min workers before starting (need to debug why it doesn't work, maybe escaping problem?)
# DRIVER_DEPENDENCY="started(${WORKER_JOB_NAME}) && numrun(${WORKER_JOB_NAME}, @{min_worker_nodes}"

DRIVER_DEPENDENCY="started(${WORKER_JOB_NAME})"

bsub @{common_job_args} -W @{job_runtime_limit} \
  -J ${DRIVER_JOB_NAME} -w "${DRIVER_DEPENDENCY}" -ti -n@{driver_slots} \
  -o "${DRIVER_LOG}" \
  "@{run_scripts_dir}/04-launch-driver.sh"

# do not redirect shutdown job output so that it gets emailed instead (notifying user of completion)
bsub @{common_job_args} -W 0:30 \
  -J ${SHUTDOWN_JOB_NAME} -w "ended(${DRIVER_JOB_NAME})" -ti -n1 \
  "@{run_scripts_dir}/05-shutdown-lsf-jobs.sh" "${DRIVER_LOG}" ${REVERSE_ORDERED_JOB_NAMES}

bjobs -X -J '@{job_name_prefix}*'

} 2>&1 | tee -a "@{run_logs_dir}"/00-queue-lsf-jobs.log