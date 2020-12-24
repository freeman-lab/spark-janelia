#!/bin/bash

# ----------------------------------------------------------------------------
# This script launches the master.
#
# The template populated parameters are:
#   run_config_dir:  @{run_config_dir}
#   spark_home:      @{spark_home}
# ----------------------------------------------------------------------------

set -e

# export spark config directory so that it is used by spark-class script
export SPARK_CONF_DIR="@{run_config_dir}"

SPARK_HOME="@{spark_home}"

CMD="${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master"

echo "$(date) [${HOSTNAME}] running ${CMD}"
${CMD}