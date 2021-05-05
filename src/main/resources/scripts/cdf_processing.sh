#!/bin/bash
#-------------------------------------------------------------------------------
# Purpose: Run the app code specific data pipeline in Spark
# Usage: cdf_processing.sh [app_code] [cob]
#-------------------------------------------------------------------------------

if [ "$#" -ne 1 ] && [ "$#" -ne 2 ] && [ "$#" -ne 6 ] ; then
  echo "Usage:"
  echo "cdf_processing.sh [app_code] [cob/closed_business_date]"
  echo "cdf_processing.sh [app_code] [cob/closed_business_date] [spark_executor] [spark_cores] [spark_executor_memory] [spark_driver_memory]"
  exit
fi

SCRIPT_PATH=$(readlink -f $0)
SCRIPT_DIR=$(dirname $SCRIPT_PATH)
SCRIPT_NM=$(basename $0)
APP_CODE=${1}
COB_LIST=${2:-1900-01-01}
NUM_EXECUTORS=${3:-10}
EXECUTOR_CORES=${4:-4}
EXECUTOR_MEMORY=${5:-6G}
DRIVER_MEMORY=${6:-10G}

CONF_FILE=app_${APP_CODE}.yml

cd ${SCRIPT_DIR}/..
APP_DIR=$PWD
cd ${APP_DIR}/..

ENV_PROPERTIES_DIR=$PWD/env
APP_LIB_DIR=$APP_DIR/lib
APP_JAR_DIR=$APP_DIR/jar
APP_CONF_DIR=$APP_DIR/conf
JOB_SPECIFIC_ENV_PROPERTIES=$APP_CONF_DIR/env.properties
APP_MAIN_CLASS=com.datafibers.cdf.Main

APP_JAR=$(ls $APP_JAR_DIR/*.jar)
EXTRA_JARS=$(ls $APP_LIB_DIR | grep -v ${APP_JAR} | xargs | tr ' ' ',' #exclude app jar
CURRENT_USER=$(whoami)
KEYTAB_LOCATION=/home/${CURRENT_USER}/${CURRENT_USER}.headless.keytab
KERBEROS_OPTS="--conf \"spark.yarn.principle=${CURRENT_USER}\" --conf \"spark.yarn.keytab=${KEYTAB_LOCATION}\""
DE_YARN_QUEUE=${DE_YARN_QUEUE:-yarn_queue_big_data}

if [ -f "$JOB_SPECIFIC_ENV_PROPERTIES" ] ; then
  ENV_PROPERTIES_DIR=$APP_CONF_DIR
fi

grep "sql_casesensitive: true" ${APP_CONF_DIR}/${CONF_FILE}
if [[ "$?" -eq "0" ]] ; then
  SPARK_SQL_CASESENSITIVE=true
else
  SPARK_SQL_CASESENSITIVE=false
fi

source ${ENV_PROPERTIES_DIR}/env.properties

cd ${APP_DIR}/scripts
# separate cob and additional parameters
COB_PART=$(echo ${COB_LIST} | cut -d ',' -f 1)
if [[ "${COB_LIST}" == *","* ]] ; then
  PAR_PART=$(echo ${COB_LIST} | cut -d ',' -f 2-)
else
  PAR_PART=""
fi

# set additional parameters as variables
par_cnt=1
for PAR in $(echo ${PAR_PART} | sed "s/,/ /g")
do
  eval para_${para_cnt}=${PAR}
  (( par_cnt++ ))
done

for PAR_PRE in $(echo ${PAR_PART} | sed "s/+/ /g")
do
  if [[ "${COB_LIST}" == *','* ]] ; then
    COB=${COB_PRE},${COB_PART} # add additional parameters back to each COB with , only if input has ,
  else
    COB=${COB_PRE}
  fi

  if [[ "${CONFIG_SPARK_MASTER}" == "local" ]] ; then
    SPARK_CMD="spark-submit --jars ${EXTRA_JARS} --name \"${APP_CODE} processing ${DE_APP_ID} ${COB_PRE}\" \
    --conf spark.sql.caseSensitive=${SPARK_SQL_CASESENSITIVE} \
    ${KERBEROS_OPTS} \
    --files /usr/hdp/current/spark2-client/conf/hive-site.xml,${APP_CONF_DIR}/log4j.properties,${ENV_PROPERTIES_DIR}/env.properties \
    --num-executors ${NUM_EXECUTORS} --executor-cores ${EXECUTOR_CORES} --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} \
    --class ${APP_MAIN_CLASS} ${APP_JAR} ${APP_CONF_DIR}/${CONF_FILE} ${COB}"
  else
    SPARK_CMD="spark-submit --jars ${EXTRA_JARS} --name \"${APP_CODE} processing ${DE_APP_ID} ${COB_PRE}\" \
    --conf spark.sql.caseSensitive=${SPARK_SQL_CASESENSITIVE} \
    ${KERBEROS_OPTS} \
    --files /usr/hdp/current/spark2-client/conf/hive-site.xml,${APP_CONF_DIR}/${CONF_FILE}#${CONF_FILE},${APP_CONF_DIR}/log4j.properties#log4j,${ENV_PROPERTIES_DIR}/env.properties#env.properties \
    --master yarn-cluster --queue ${DE_YARN_QUEUE} \
    --num-executors ${NUM_EXECUTORS} --executor-cores ${EXECUTOR_CORES} --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} \
    --class ${APP_MAIN_CLASS} ${APP_JAR} ${CONF_FILE} ${COB}"
  fi

export SPARK_MAJOR_VERSION=2
SPARK_CMD="$SPARK_CMD"
echo "Executing on cob=${COB} with Spark submit cmd=$SPARK_CMD"
eval $SPARK_CMD

if [[ "$?" -eq "0" ]] ; then
  echo "Spark process in script $SCRIPT_NM completed successfully."
else
  echo "Spark process in script $SCRIPT_NM failed."
fi

















