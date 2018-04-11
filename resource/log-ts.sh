#!/usr/bin/env bash
# export  YARN_CONF_DIR=/opt/cloudera/parcels/CDH/lib/spark
spark-submit \
--master yarn \
--driver-memory 8g \
--num-executors 25 \
--executor-memory 10g \
--executor-cores 4 \
log-timeseries.jar \
'/user/shijia/netLog2016_ZhongXinTong_20180[2-3][0-3][0-9]_merge.ok' \
/user/shijia/netResult

