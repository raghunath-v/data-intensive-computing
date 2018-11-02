#!/bin/bash

file=$1
log_file="log.txt"


if [[ -z "$file" ]]; then
    	echo "argument error"
	exit 1
fi

export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

hdfs dfs -put youtubedata.txt .
hdfs dfs -rm -r output 

DIR=$file"_classes"
if [ -d "$DIR" ]; then
	rm -r $DIR
fi
mkdir $DIR

javac -cp $HADOOP_CLASSPATH -d $DIR $file.java

jar -cvf $file.jar -C $DIR .

hadoop jar $file.jar project.$file youtubedata.txt output

hdfs dfs -cat output/part-r-00000 | sort -n -k2 -r | head -n5
