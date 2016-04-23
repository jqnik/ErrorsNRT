export TOOLNAME=ErrorsNRT-shaded

#
# Runtime parameter to controle the tool functions.
#
export YARN_MODE=yarn-client
#export YARN_MODE=local[2]

export OUT=/projects/ErrorsNRT/out/events
export AGG=/projects/ErrorsNRT/errorsNRT_table
export CP=/projects/ErrorsNRT/checkpoints
export FLUME_HOST=yourHost.fq.dn
export FLUME_PORT=7777
export BATCH_SECONDS=5
export SLIDE_SECONDS=30
export WINDOW_SECONDS=30
export NUM_STREAMS=2

#cd /opt/cloudera/me-1.0.0/partsmatching_use_case/ && PROJECT_HOME=`pwd` && \
PROJECT_HOME=`pwd` && \
spark-submit \
--conf spark.driver.extraClassPath=/etc/hive/conf:/opt/cloudera/parcels/CDH/hive/lib/hive/* \
--conf spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/hive/lib/hive/* \
--conf spark.yarn.am.cores=1 \
--executor-cores $NUM_STREAMS \
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
--numStreams $NUM_STREAMS \
--verbose true &
