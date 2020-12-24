# ----------------------------------------------------------------------------
# Spark env template can be found in:
#   ${SPARK_HOME}/conf/spark-env.sh.template
#
# The template populated parameters are:
#   java_home:      @{java_home}
#   run_local_dirs: @{run_local_dirs}
#   run_logs_dir:   @{run_logs_dir}
#   run_worker_dir: @{run_worker_dir}
#   spark_home:     @{spark_home}
#   worker_memory:  @{worker_memory}
#   worker_opts:    @{worker_opts}
# ----------------------------------------------------------------------------

export JAVA_HOME="@{java_home}"
export SPARK_HOME="@{spark_home}"

# storage directories to use on this node for shuffle and RDD data
export SPARK_LOCAL_DIRS="@{run_local_dirs}"

# where log files are stored (default: SPARK_HOME/logs)
export SPARK_LOG_DIR="@{run_logs_dir}"

# working directory of worker processes
export SPARK_WORKER_DIR="@{run_worker_dir}"

# config properties only for the worker (e.g. -Dx=y)
#   spark.worker.cleanup.enabled=true causes worker to remove SPARK_WORKER_DIR (with worker log data) before exit
export SPARK_WORKER_OPTS="@{worker_opts}"

# total memory available for all executors on a spark worker/node (e.g. 1000m, 2g)
export SPARK_WORKER_MEMORY="@{worker_memory}g"

export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"