#!/bin/bash
#-------------------------------------------------------------------------------
# Purpose: Compare two dataframes using specified join keys and print out result
# Usage: cdf_compare.sh [expectDFPath] [actualDFPath] [joinKey] [rateThreshold]
#-------------------------------------------------------------------------------

if [ "$#" -ne 5 ] && [ "$#" -ne 9 ] ; then
  echo "Usage:"
  echo "cdf_compare.sh [expectDFPath] [actualDFPath] [joinKey] [rateThreshold] [hdfsOutputPath]"
  echo "cdf_compare.sh [expectDFPath] [actualDFPath] [joinKey] [rateThreshold] [hdfsOutputPath] [spark_executor] [spark_cores] [spark_executor_memory] [spark_driver_memory]"
  exit
fi

SCRIPT_PATH=$(readlink -f $0)
SCRIPT_DIR=$(dirname $SCRIPT_PATH)
SCRIPT_NM=$(basename $0)
EXP_DF_PATH=${1}
ACT_DF_PATH=${2}
JOIN_KEY=${3}
RATE_THRESHOLD=${4:-0}
HDFS_OUTPUT_PATH=${5}
NUM_EXECUTORS=${6:-10}
EXECUTOR_CORES=${7:-4}
EXECUTOR_MEMORY=${8:-6G}
DRIVER_MEMORY=${9:-10G}

cd ${SCRIPT_DIR}/..
APP_DIR=$PWD
cd ${APP_DIR}/..
ENV_PROPERTIES_DIR=$PWD/env
APP_LIB_DIR=$APP_DIR/lib
APP_JAR_DIR=$APP_DIR/jar
APP_CONF_DIR=$APP_DIR/conf

APP_MAIN_CLASS=com.datafibers.cdf.utils.DFCompare

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

if [[ "${CONFIG_SPARK_MASTER}" == "local" ]] ; then
  SPARK_CMD="spark-submit --jars ${EXTRA_JARS} --name \"CDF Data Comparing - ${EXP_DF_PATH}\" \
  --conf spark.sql.codegen.wholeStage=false \
  --conf spark.sql.caseSensitive=true \
  ${KERBEROS_OPTS} \
  --files /usr/hdp/current/spark2-client/conf/hive-site.xml,${APP_CONF_DIR}/log4j.properties,${ENV_PROPERTIES_DIR}/env.properties \
  --num-executors ${NUM_EXECUTORS} --executor-cores ${EXECUTOR_CORES} --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} \
  --class ${APP_MAIN_CLASS} ${APP_JAR} ${EXP_DF_PATH} ${ACT_DF_PATH} ${JOIN_KEY} ${RATE_THRESHOLD} ${HDFS_OUTPUT_PATH}"
else
  SPARK_CMD="spark-submit --jars ${EXTRA_JARS} --name \"CDF Data Comparing - ${EXP_DF_PATH}\" \
  --conf spark.sql.codegen.wholeStage=false \
  --conf spark.sql.caseSensitive=true \
  ${KERBEROS_OPTS} \
  --files /usr/hdp/current/spark2-client/conf/hive-site.xml,${APP_CONF_DIR}/log4j.properties#log4j,${ENV_PROPERTIES_DIR}/env.properties#env.properties \
  --master yarn-cluster --queue ${DE_YARN_QUEUE} \
  --num-executors ${NUM_EXECUTORS} --executor-cores ${EXECUTOR_CORES} --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} \
  --class ${APP_MAIN_CLASS} ${APP_JAR} ${EXP_DF_PATH} ${ACT_DF_PATH} ${JOIN_KEY} ${RATE_THRESHOLD} ${HDFS_OUTPUT_PATH}"
fi

export SPARK_MAJOR_VERSION=2
SPARK_CMD="$SPARK_CMD"
eval $SPARK_CMD

if [[ "$?" -eq "0" ]] ; then
  echo "Spark process in script $SCRIPT_NM completed successfully."
else
  echo "Spark process in script $SCRIPT_NM failed."
fi

















