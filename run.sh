#########################################################################
# File Name: run.sh
# Author: Leon Lee
# mail: lee.leon0519@gmail.com
# Created Time: Wed 13 May 2015 12:15:59 PM CST
# Description: This script is used to run spark application on different 
#			   platforms. You may only run on one platform each time.
#########################################################################
#!/bin/bash


# Run Application on YARN
if false; then
export YARN_CONF_DIR=/usr/local/hadoop-2.5.2/etc/hadoop
SPARK_JAR=/usr/local/spark-1.3.1/lib/spark-assembly-1.3.1-hadoop2.4.0.jar
/usr/local/spark-1.3.1/bin/spark-class org.apache.spark.deploy.yarn.Client \
--jar /root/workplace/Hadoop-Devel/loganalyse/target/loganalyse-1.0.0.jar \
--class com.leon.hadoop.loganalyse.LogStatistic \
--num-workers 2 \
--master-memory 1g \
--worker-memory 1g \
--worker-cores 1
fi

# Run Application on Spark
if false; then
/usr/local/spark-1.3.1/bin/spark-submit \
--name SparkTestJob \
--class com.leon.hadoop.loganalyse.LogStatistic \
--master spark://homeyard.com:7077 \
--executor-memory 512m \
--total-executor-cores 1 \
/root/workplace/Hadoop-Devel/loganalyse/target/loganalyse-1.0.0.jar
fi

classpath=`find /root/.m2/repository/ -type f -name '*.jar' |grep -Ev "sources|javadoc" | tr '\n' ':'`
#java -cp ./target/loganalyse-1.0.0.jar com.leon.hadoop.loganalyse.App
