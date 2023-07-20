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

echo "$(date) [${HOSTNAME}] submitting jobs to scheduler"

MASTER_JOB_ID=$(qsub @{common_job_args} -terse -l h_rt=@{job_max_run_seconds} -N ${MASTER_JOB_NAME} -pe mpi 1 -j y -V -o "${MASTER_LOG}" "@{run_scripts_dir}/01-launch-master.sh")
echo "submitted job ${MASTER_JOB_ID} with name ${MASTER_JOB_NAME}"

# SGE does not support "started" job dependencies, so start polling master log right away and poll for longer
MAX_ATTEMPTS=20 # each attempt is 30 seconds, so this will give the master 10 minutes to start
MAX_POLL_SECONDS=$(( (MAX_ATTEMPTS + 1) * 30 ))

URL_JOB_ID=$(qsub @{common_job_args} -terse -l h_rt=${MAX_POLL_SECONDS} -N ${URL_JOB_NAME} -pe mpi 1 -j y -V -o "@{run_logs_dir}/02-url.log" "@{run_scripts_dir}/02-save-master-url.sh" "${MASTER_LOG}" ${MAX_ATTEMPTS})
echo "submitted job ${URL_JOB_ID} with name ${URL_JOB_NAME}"

# SGE does not support "started" job dependencies, so start polling qstat as soon as master URL is saved
# and before launching workers (to prevent poll job from getting starved out by workers)
POLLING_INTERVAL_SECONDS=30
WORKER_READY_JOB_ID=$(qsub @{common_job_args} -terse -l h_rt=@{job_max_run_seconds} -N ${WORKER_READY_JOB_NAME} -pe mpi 1 -j y -V -o "@{run_logs_dir}/11-verify-workers-ready.log" "@{run_scripts_dir}/11-verify-sge-workers-ready.sh" ${WORKER_JOB_ID} ${MAX_ATTEMPTS} ${POLLING_INTERVAL_SECONDS} @{min_worker_nodes} @{max_worker_nodes})
echo "submitted job ${WORKER_READY_JOB_ID} with name ${WORKER_READY_JOB_NAME}"

# include $TASK_ID "pseudo environment variable" in worker log name so that output from multiple workers are separated
WORKER_JOB_ID=$(qsub @{common_job_args} -terse -l h_rt=@{job_max_run_seconds} -N ${WORKER_JOB_NAME} -t 1-@{max_worker_nodes} -hold_jid ${URL_JOB_ID} -pe mpi @{worker_slots} -j y -V -o "@{run_logs_dir}/worker-\$TASK_ID-launch.log" "@{run_scripts_dir}/03-launch-worker.sh")
echo "submitted job ${WORKER_JOB_ID} with name ${WORKER_JOB_NAME}"

DRIVER_JOB_ID=$(qsub @{common_job_args} -terse -l h_rt=@{job_max_run_seconds} -N ${DRIVER_JOB_NAME} -hold_jid ${WORKER_READY_JOB_ID}  -pe mpi @{driver_slots} -j y -V -o "${DRIVER_LOG}" "@{run_scripts_dir}/04-launch-driver.sh")
echo "submitted job ${DRIVER_JOB_ID} with name ${DRIVER_JOB_NAME}"

# do not redirect shutdown job output so that it gets emailed instead (notifying user of completion)
REVERSE_ORDERED_JOB_IDS="${DRIVER_JOB_ID} ${WORKER_JOB_ID} ${WORKER_READY_JOB_ID} ${URL_JOB_ID} ${MASTER_JOB_ID}"
SHUTDOWN_JOB_ID=$(qsub @{common_job_args} -terse -l h_rt=1800 -N ${SHUTDOWN_JOB_NAME} -hold_jid ${DRIVER_JOB_ID} -pe mpi 1 -j y -V "@{run_scripts_dir}/12-shutdown-sge-jobs.sh" "${DRIVER_LOG}" ${REVERSE_ORDERED_JOB_IDS})
echo "submitted job ${SHUTDOWN_JOB_ID} with name ${SHUTDOWN_JOB_NAME}

"

qstat | grep -E "${MASTER_JOB_ID}|${URL_JOB_ID}|${WORKER_READY_JOB_ID}|${WORKER_JOB_ID}|${DRIVER_JOB_ID}|${SHUTDOWN_JOB_ID}"

} 2>&1 | tee -a "@{run_logs_dir}"/00-queue-sge-jobs.log