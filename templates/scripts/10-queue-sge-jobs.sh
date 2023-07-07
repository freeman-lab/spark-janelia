#!/bin/bash

# ----------------------------------------------------------------------------
# This script submits all jobs to the SGE cluster scheduler.
#
# The template populated parameters are:
#   common_job_args:      @{common_job_args}
#   driver_slots:         @{driver_slots}
#   job_max_run_seconds:  @{job_max_run_seconds}
#   job_name_prefix:      @{job_name_prefix}
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
WORKER_READY_JOB_NAME="@{job_name_prefix}_re"
WORKER_JOB_NAME="@{job_name_prefix}_wo"
DRIVER_JOB_NAME="@{job_name_prefix}_dr"
SHUTDOWN_JOB_NAME="@{job_name_prefix}_sd"

MASTER_LOG="@{run_logs_dir}/01-master.log"
DRIVER_LOG="@{run_logs_dir}/04-driver.log"

REVERSE_ORDERED_JOB_NAMES="${DRIVER_JOB_NAME} ${WORKER_JOB_NAME} ${WORKER_READY_JOB_NAME} ${URL_JOB_NAME} ${MASTER_JOB_NAME}"

echo "$(date) [${HOSTNAME}] submitting jobs to scheduler"

qsub @{common_job_args} -l h_rt=@{job_max_run_seconds} \
  -N ${MASTER_JOB_NAME} -pe mpi 1 \
  -j y -o "${MASTER_LOG}" \
  "@{run_scripts_dir}/01-launch-master.sh"

# SGE does not support "started" job dependencies, so start polling master log right away and poll for longer
MAX_ATTEMPTS=20 # each attempt is 30 seconds, so this will give the master 10 minutes to start
MAX_POLL_SECONDS=$(( (MAX_ATTEMPTS + 1) * 30 ))

qsub @{common_job_args} -l h_rt=${MAX_POLL_SECONDS} \
  -N ${URL_JOB_NAME} -pe mpi 1 \
  -j y -o "@{run_logs_dir}/02-url.log" \
  "@{run_scripts_dir}/02-save-master-url.sh" "${MASTER_LOG}" ${MAX_ATTEMPTS}

# SGE does not support "started" job dependencies, so start polling qstat as soon as master URL is saved
# and before launching workers (to prevent poll job from getting starved out by workers)
POLLING_INTERVAL_SECONDS=30
qsub @{common_job_args} -l h_rt=@{job_max_run_seconds} \
  -N ${WORKER_READY_JOB_NAME} -pe mpi 1 \
  -j y -o "@{run_logs_dir}/11-verify-workers-ready.log" \
  "@{run_scripts_dir}/11-verify-sge-workers-ready.sh" \
  ${WORKER_JOB_NAME} ${MAX_ATTEMPTS} ${POLLING_INTERVAL_SECONDS} @{min_worker_nodes}

# include $TASK_ID in worker log name so that output from multiple workers are separated
qsub @{common_job_args} -l h_rt=@{job_max_run_seconds} \
  -N ${WORKER_JOB_NAME} -t 1-@{max_worker_nodes} -hold_jid ${URL_JOB_NAME} -pe mpi @{worker_slots} \
  -j y -o "@{run_logs_dir}/worker-\$TASK_ID-launch.log" \
  "@{run_scripts_dir}/03-launch-worker.sh"

qsub @{common_job_args} -l h_rt=@{job_max_run_seconds} \
  -N ${DRIVER_JOB_NAME} -hold_jid ${WORKER_READY_JOB_NAME}  -pe mpi @{driver_slots} \
  -j y -o "${DRIVER_LOG}" \
  "@{run_scripts_dir}/04-launch-driver.sh"

# do not redirect shutdown job output so that it gets emailed instead (notifying user of completion)
qsub @{common_job_args} -l h_rt=1800 \
  -N ${SHUTDOWN_JOB_NAME} -hold_jid ${DRIVER_JOB_NAME} -pe mpi 1 \
  -j y \
  "@{run_scripts_dir}/12-shutdown-sge-jobs.sh" "${DRIVER_LOG}" ${REVERSE_ORDERED_JOB_NAMES}

JOB_NAME_PREFIX="@{job_name_prefix}"
SHORT_JOB_NAME_PREFIX="${JOB_NAME_PREFIX:0:8}"
qstat | grep "${SHORT_JOB_NAME_PREFIX}"

} 2>&1 | tee -a "@{run_logs_dir}"/00-queue-sge-jobs.log