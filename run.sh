#!/bin/bash

NAME=$1
BASE=./runners

CURR=$PWD

TARGETDIR=${BASE}

STARKLIB=${BASE}/../stark.jar
#GEOSPARKLIB=${BASE}/../geospark-0.3.jar #geospark-0.3.2-spark-2.x.jar

LIB=$STARKLIB

cd $TARGETDIR

rm -f *.class
echo "`date` compiling ${TARGETDIR}/{StatsCollector,${NAME}}.scala"
scalac -cp .:$SPARK_ASSEMBLY:$LIB {StatsCollector,${NAME}}.scala
result=$?

if [ "$result" != 0 ]
then
	echo "exit code of scala compiler $result"
	exit $result
fi
echo "`date` create jar"
jar -cf ../runners.jar *
rm -f *.class


echo "`date` submit job"
time spark-submit  --master yarn --class $NAME --num-executors 32 --executor-cores 2 --executor-memory 7g --conf spark.network.timeout=1800000 --jars $LIB ../runners.jar

cd $CURR
