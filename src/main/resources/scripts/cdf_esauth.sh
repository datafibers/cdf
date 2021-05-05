#!/bin/bash
#-------------------------------------------------------------------------------
# Purpose: Fetch elastic authentication from encrypted user/pass files
# Usage: cdf_esauth.sh
#-------------------------------------------------------------------------------

SCRIPT_PATH=$(readlink -f $0)
SCRIPT_DIR=$(dirname $SCRIPT_PATH)
SCRIPT_NM=$(basename $0)

cd ${SCRIPT_DIR}/..
APP_DIR=$PWD
cd ${APP_DIR}/..

ENV_PROPERTIES_DIR=$PWD/env
APP_LIB_DIR=$APP_DIR/lib
APP_JAR_DIR=$APP_DIR/jar
APP_CONF_DIR=$APP_DIR/conf

APP_MAIN_CLASS=com.datafibers.cdf.utils.ESAuth

APP_JAR=$(ls $APP_JAR_DIR/*.jar)
EXTRA_JARS=$(ls $APP_LIB_DIR | grep -v ${APP_JAR} | xargs | tr ' ' ',' #exclude app jar

CURRENT_USER=$(whoami)
KEYTAB_LOCATION=/home/${CURRENT_USER}/${CURRENT_USER}.headless.keytab
KERBEROS_OPTS="--conf \"spark.yarn.principle=${CURRENT_USER}\" --conf \"spark.yarn.keytab=${KEYTAB_LOCATION}\""
DE_YARN_QUEUE=${DE_YARN_QUEUE:-yarn_queue_big_data}

if [ -f "$JOB_SPECIFIC_ENV_PROPERTIES" ] ; then
  ENV_PROPERTIES_DIR=$APP_CONF_DIR
fi

source ${ENV_PROPERTIES_DIR}/env.properties

cd ${APP_DIR}/scripts
SPARK_CMD="spark-submit --jars ${EXTRA_JARS} ${KERBEROS_OPTS} --files ${ENV_PROPERTIES_DIR}/env.properties --class ${APP_MAIN_CLASS} ${APP_JAR}"

export SPARK_MAJOR_VERSION=2
SPARK_CMD="$SPARK_CMD"
eval $SPARK_CMD

if [[ "$?" -eq "0" ]] ; then
  echo "Spark process in script $SCRIPT_NM completed successfully."
else
  echo "Spark process in script $SCRIPT_NM failed."
fi

















