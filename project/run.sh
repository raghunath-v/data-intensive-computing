#!/bin/sh

export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

hdfs dfs -put youtubedata.txt .

DIR='viral_classes'
if [ -d "$DIR" ]; then
	rm -r $DIR
fi
mkdir $DIR

javac -cp $HADOOP_CLASSPATH -d $DIR Viral.java

jar -cvf viral.jar -C $DIR .

hadoop jar viral.jar project.Viral youtubedata.txt output
