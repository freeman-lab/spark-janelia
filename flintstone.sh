#!/bin/bash

#---------------------------------------------------------------------------------
# Wrapper script for calling spark-janelia. The script has been adapted from
# https://github.com/saalfeldlab/flintstone/blob/master/flintstone-lsd.sh
# to work with the recently updated spark-janelia script.
#---------------------------------------------------------------------------------

CONTAINING_DIRECTORY=$( dirname "${BASH_SOURCE[0]}" )
SPARK_JANELIA=${SPARK_JANELIA:-${CONTAINING_DIRECTORY}/spark-janelia}
RUNTIME=${RUNTIME:-8:00}
MIN_WORKERS=${MIN_WORKERS:-1}
N_EXECUTORS_PER_NODE=${N_EXECUTORS_PER_NODE:-6}
N_CORES_PER_EXECUTOR=${N_CORES_PER_EXECUTOR:-5}

# To distribute work evenly, recommended number of tasks/partitions is 3 times the number of cores.
N_TASKS_PER_EXECUTOR_CORE=${N_TASKS_PER_EXECUTOR_CORE:-3}

# Each Janelia Spark worker node used to have 30 executor cores + 2 overhead (hadoop) cores.
# These new variables allow callers to configure different setups.
N_OVERHEAD_CORES_PER_WORKER=${N_OVERHEAD_CORES_PER_WORKER:-2}
DEFAULT_CORES_PER_WORKER=$(( (N_EXECUTORS_PER_NODE * N_CORES_PER_EXECUTOR) + N_OVERHEAD_CORES_PER_WORKER ))
N_CORES_PER_WORKER=${N_CORES_PER_WORKER:-${DEFAULT_CORES_PER_WORKER}}

# The flintstone script used to always allocate a 32 core driver, but that can now be changed with this variable.
# When overriding the default, consider that driver_memory = driver_cores * gb_per_slot.
N_CORES_DRIVER=${N_CORES_DRIVER:-32}

# Janelia cluster resources per slot are documented here:
#   http://wiki.int.janelia.org/wiki/display/ScientificComputing/Janelia+Compute+Cluster
GB_PER_SLOT=${GB_PER_SLOT:-15}

# consolidate logs by default until worker cleanup issues are fixed for multi-stage spark applications
SPARK_JANELIA_ARGS="${SPARK_JANELIA_ARGS:---consolidate_logs}"

SPARK_JANELIA_TASK=${SPARK_JANELIA_TASK:-generate-and-launch-run}
CONDA_ACTIVATE=${CONDA_ACTIVATE:-/groups/flyTEM/flyTEM/anaconda2/bin/activate}
CONDA_ENV=${CONDA_ENV:-py36}

if [ "$#" -lt "3" ]; then
    SCRIPT=$(basename $0)
    echo """
USAGE: ${SCRIPT} <number of worker nodes> <spark-submit jar> <spark-submit class> [spark-submit args]

Use these environment variables to override defaults:

Name                         Description                                              Current Value
---------------------------  -------------------------------------------------------  -------------
CONDA_ACTIVATE               conda activate path (to setup spark-janelia python env)  ${CONDA_ACTIVATE}
CONDA_ENV                    conda environment name                                   ${CONDA_ENV}
GB_PER_SLOT                  memory (GB) allocated per cluster slot                   ${GB_PER_SLOT}
JAVA_HOME                    java install location                                    ${JAVA_HOME}
LSF_PROJECT                  project for cluster job billing                          ${LSF_PROJECT}
MIN_WORKERS                  minimum number of worker nodes before starting driver    ${MIN_WORKERS}
N_CORES_DRIVER               number of cores to allocate for driver node              ${N_CORES_DRIVER}
N_CORES_PER_EXECUTOR         number of cores to allocate for each executor            ${N_CORES_PER_EXECUTOR}
N_CORES_PER_WORKER           number of cores to allocate for each worker node         ${N_CORES_PER_WORKER}
N_EXECUTORS_PER_NODE         number of executors to allocate to each worker node      ${N_EXECUTORS_PER_NODE}
N_OVERHEAD_CORES_PER_WORKER  number of overhead cores to add to each worker node      ${N_OVERHEAD_CORES_PER_WORKER}
N_TASKS_PER_EXECUTOR_CORE    number of tasks to allocate to each executor core        ${N_TASKS_PER_EXECUTOR_CORE}
OVERRIDE_PARALLELISM         explicit parallelism value (overrides derived value)     ${OVERRIDE_PARALLELISM}
RUNTIME                      hard runtime threshold for jobs                          ${RUNTIME}
SPARK_HOME                   spark install location                                   ${SPARK_HOME}
SPARK_JANELIA                path to spark-janelia script                             ${SPARK_JANELIA}
SPARK_JANELIA_ARGS           additional args to pass to spark-janelia                 ${SPARK_JANELIA_ARGS}
SPARK_JANELIA_TASK           spark-janelia task (e.g. generate-run)                   ${SPARK_JANELIA_TASK}
SUBMIT_ARGS                  additional --conf args to pass to spark-submit           ${SUBMIT_ARGS}
""" 1>&2
    exit 1
fi

N_NODES=$1;            shift
JAR=$(readlink -f $1); shift
CLASS=$1;              shift
ARGV="$@"

MEMORY_PER_EXECUTOR=$(( N_CORES_PER_EXECUTOR * GB_PER_SLOT ))
N_EXECUTORS=$(( N_NODES *  N_EXECUTORS_PER_NODE ))
PARALLELISM=$(( N_EXECUTORS * N_CORES_PER_EXECUTOR * N_TASKS_PER_EXECUTOR_CORE ))

if [ -n "${OVERRIDE_PARALLELISM}" ]; then
  PARALLELISM="${OVERRIDE_PARALLELISM}"
fi

SUBMIT_ARGS="${SUBMIT_ARGS} --verbose"
SUBMIT_ARGS="${SUBMIT_ARGS} --conf spark.default.parallelism=$PARALLELISM"
SUBMIT_ARGS="${SUBMIT_ARGS} --conf spark.driver.cores=${N_CORES_DRIVER}"
SUBMIT_ARGS="${SUBMIT_ARGS} --conf spark.executor.instances=$N_EXECUTORS_PER_NODE"
SUBMIT_ARGS="${SUBMIT_ARGS} --conf spark.executor.cores=$N_CORES_PER_EXECUTOR"
SUBMIT_ARGS="${SUBMIT_ARGS} --conf spark.executor.memory=${MEMORY_PER_EXECUTOR}g"
SUBMIT_ARGS="${SUBMIT_ARGS} --class $CLASS"
SUBMIT_ARGS="${SUBMIT_ARGS} ${JAR}"
SUBMIT_ARGS="${SUBMIT_ARGS} ${ARGV}"

[[ -n ${LSF_PROJECT} ]] && PROJECT="--lsf_project='${LSF_PROJECT}'" || unset PROJECT

[[ -f "${CONDA_ACTIVATE}" ]] && source "${CONDA_ACTIVATE}" ${CONDA_ENV}

CMD="${SPARK_JANELIA} ${PROJECT} ${SPARK_JANELIA_ARGS}"
CMD="${CMD} --nnodes=${N_NODES} --gb_per_slot=${GB_PER_SLOT} --driverslots=${N_CORES_DRIVER}"
CMD="${CMD} --worker_slots=${N_CORES_PER_WORKER} --minworkers=${MIN_WORKERS} --hard_runtime=${RUNTIME}"
CMD="${CMD} --submitargs=\"${SUBMIT_ARGS}\" ${SPARK_JANELIA_TASK}"

echo """
On ${HOSTNAME} with $(python -V 2>&1), running:

  ${CMD}
"""

eval ${CMD}