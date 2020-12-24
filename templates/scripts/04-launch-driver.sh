#!/bin/bash

# ----------------------------------------------------------------------------
# This script launches the driver.
#
# The template populated parameters are:
#   master_url_path: @{master_url_path}
#   run_config_dir:  @{run_config_dir}
#   spark_home:      @{spark_home}
#   submit_args:     @{submit_args}
# ----------------------------------------------------------------------------

set -e

# export spark config directory so that it is used by spark-submit script
export SPARK_CONF_DIR="@{run_config_dir}"

SPARK_HOME="@{spark_home}"
MASTER_URL=$(cat "@{master_url_path}")

CMD="${SPARK_HOME}/bin/spark-submit --deploy-mode client @{submit_args}"

echo "$(date) [${HOSTNAME}] running ${CMD}"
${CMD}