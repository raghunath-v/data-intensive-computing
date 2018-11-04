# How to run the applications:


1. Start the Hadoop Namenode and Datanode using the following commands.

```
$HADOOP_HOME/bin/hadoop-daemon.sh start namenode
$HADOOP_HOME/bin/hadoop-daemon.sh start datanode
```

2. To see the top 5 categories with the most average views per video, run;

```
./run.sh AvgViews
```

3. The results are also dumped in the script.js file which can be viewed in a browser like Firefox.

```
firefox index.html &
```

4. To see the top 5 uploders in the dataset, run;

```
./run.sh Top5uploaders
```

The results will be shown in the terminal window.
