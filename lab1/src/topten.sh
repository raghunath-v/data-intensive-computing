option=$1

case $option in
	-n)
		$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
		$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
		cd topten
		$HADOOP_HOME/bin/hdfs dfs -mkdir -p topten_input
		$HADOOP_HOME/bin/hdfs dfs -put data/users.xml topten_input/
		$HADOOP_HOME/bin/hdfs dfs -ls topten_input
		$HBASE_HOME/bin/start-hbase.sh
		export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
		export HBASE_CLASSPATH=$($HBASE_HOME/bin/hbase classpath)
		export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HBASE_CLASSPATH
	;;
	-r)
		javac -cp $HADOOP_CLASSPATH -d topten_classes topten/TopTen.java
		jar -cvf topten.jar -C topten_classes/ .
		hdfs dfs -rm -r topten_output
		$HADOOP_HOME/bin/hadoop jar topten.jar topten.TopTen topten_input topten_output
	;;
esac
