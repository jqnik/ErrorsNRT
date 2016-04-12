export TOOLNAME=ErrorsNRT-shaded

#
# Runtime parameter to controle the tool functions.
#
export YARN_MODE=yarn-client
#export YARN_MODE=local[2]

export OUT=/projects/ErrorsNRT/out/events
export AGG=/projects/ErrorsNRT/countsRDD
export CP=/projects/ErrorsNRT/checkpoints
export FLUME_HOST=yourHost.fq.dn
export FLUME_PORT=7777
export BATCH_SECONDS=15
export SLIDE_SECONDS=300
export WINDOW_SECONDS=300

echo
echo ">>> Running tool [$TOOLNAME] ..."
echo output location events     : $OUT
echo output location aggregates : $AGG
echo output location events     : $CP
echo flume host                 : $FLUME_HOST
echo flume port                 : $FLUME_PORT
echo micro-batch interval       : $BATCH_SECONDS
echo win-creation interval      : $SLIDE_SECONDS
echo win-length		        : $WINDOW_SECONDS
echo

#cd /opt/cloudera/me-1.0.0/partsmatching_use_case/ && PROJECT_HOME=`pwd` && \
PROJECT_HOME=`pwd` && \
spark-submit --conf spark.driver.extraClassPath=/etc/hive/conf:/opt/cloudera/parcels/CDH/hive/lib/hive/* \
--conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/hive/lib/hive/* \
--jars $PROJECT_HOME/$TOOLNAME.jar \
--master $YARN_MODE --name ErrorsNRT-$YARN_MODE \
--class com.cloudera.sa.example.errorsNRT.ErrorsNRT $TOOLNAME.jar \
--slideSeconds $SLIDE_SECONDS \
--windowSeconds $WINDOW_SECONDS \
--batchSeconds $BATCH_SECONDS \
--master $YARN_MODE \
--aggfile $AGG \
--outfile $OUT \
--cpfile $CP \
--flumeHost $FLUME_HOST \
--flumePort $FLUME_PORT \
--verbose true &
