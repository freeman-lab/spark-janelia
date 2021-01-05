#!/bin/bash

# ----------------------------------------------------------------------------
# This script launches one worker.
#
# The template populated parameters are:
#   master_url_path: @{master_url_path}
#   run_config_dir:  @{run_config_dir}
#   spark_home:      @{spark_home}
# ----------------------------------------------------------------------------

set -e

# export spark config directory so that it is used by spark-class script
export SPARK_CONF_DIR="@{run_config_dir}"

SPARK_HOME="@{spark_home}"
MASTER_URL=$(cat "@{master_url_path}")

CMD="${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker ${MASTER_URL}"

echo "$(date) [${HOSTNAME}] running ${CMD}"
${CMD}