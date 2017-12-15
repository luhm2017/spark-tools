#!/bin/bash
source /etc/profile
## usage: run one degree graph data sh date.sh -d 2017-09-04 -t 2
## usage: run two degree graph data sh date.sh -d 2017-09-04 -t 2 -y two

PROJECT_HOME="$(cd "`dirname "$0"`/.."; pwd)"
days=0
degree="one"
loop=4
stdate=${1:-`date -d '1 days ago' +"%Y-%m-%d"`}

while getopts "d:i:o:t:y:" opt ; do
 case $opt in
  d)stdate=$OPTARG ;;
  i)inputdir=$OPTARG ;;
  o)outputdir=$OPTARG ;;
  t)days=$OPTARG ;;
  y)degree=$OPTARG ;;
  ?)echo "==> please input arg: stdate(d), inputdir(i)" && exit 1 ;;
 esac
done

inputdir=/user/szdsjkf/explortoutput/$stdate

enddate="$stdate 23:59:59"
#stdate=`date -d "$stdate -i -$days day" +"%Y-%m-%d"`
stdate="$stdate 00:00:00"

echo
echo "run $degree graph loop count $loop"
echo
echo "parma:(inputdir):$inputdir,(loop):$loop,(stdate):$stdate,(enddate):$enddate"
echo
echo "$(date +"%Y-%m-%d %H:%M:%S")   start run graph data *******  "

sh /home/szdsjkf/spark2-submit \
 --master yarn \
 --deploy-mode cluster \
 --class ExportGraphDataDay \
 --queue szoffline \
 --driver-memory 2G \
 --executor-memory 60G \
 --num-executors 10 \
 --executor-cores 10 \
 --conf spark.shuffle.compress=true \
 --conf spark.executor.extraJavaOptions="-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=15 -XX:CMSInitiatingOcc
upancyFraction=70 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
 --conf spark.shuffle.file.buffer=64 \
 --conf spark.storage.memoryFraction=0.3 \
 --conf spark.shuffle.memoryFraction=0.5 \
 --conf spark.yarn.executor.memoryOverhead=10240 \
 --conf spark.shuffle.io.retryWait=60s \
 --conf spark.shuffle.io.maxRetries=30 \
 --driver-class-path $PROJECT_HOME/dependency/mysql-connector-java-5.1.36.jar \
 --jars $PROJECT_HOME/target/original-graphx-analysis-core-1.0.0-SNAPSHOT.jar,$PROJECT_HOME/target/original-graphx-analysis-common-1.0.0-SNAPSHOT.jar,$PROJECT_HOME/depende
ncy/guava-14.0.1.jar \
 $PROJECT_HOME/target/original-graphx-analysis-apply.jar \
 $inputdir $loop "$stdate" "$enddate"

echo "$(date +"%Y-%m-%d %H:%M:%S")   end run graph data *******  "
echo "dt=`date -d "$stdate -i -4 day" +"%Y-%m-%d"`"
hive -hiveconf dt="`date -d "$stdate -i -4 day" +"%Y-%m-%d"`" -f /home/szdsjkf/yanshi.lin/bin/batchExport.q
