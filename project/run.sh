#!/bin/bash

# Get inputs
file=$1
log_file="log.txt"

# Throw error if no input is provided
if [[ -z "$file" ]]; then
    	echo "argument error"
	exit 1
fi

export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

# Store the big data file in hadoop
hdfs dfs -put youtubedata.txt .
hdfs dfs -rm -r output 

DIR=$file"_classes"
if [ -d "$DIR" ]; then
	rm -r $DIR
fi
mkdir $DIR

# Compile the java program and run it
javac -cp $HADOOP_CLASSPATH -d $DIR $file.java

jar -cvf $file.jar -C $DIR .

hadoop jar $file.jar project.$file youtubedata.txt output

# Get the output of the program
hdfs dfs -cat output/part-r-00000 | sort -n -k2 -r | head -n5

if [ "$file" = "AvgViews" ]; then

# Generate the ChartJS script for viewing data on browser
Labels="$(hdfs dfs -cat output/part-r-00000 | sort -n -k2 -r | head -n5 | sed -e 's/\s.*/"/g' | sed -e 's/^/"/g' | sed ':a;N;$!ba;s/\n/, /g')"
	
Data="$(hdfs dfs -cat output/part-r-00000 | sort -n -k2 -r | head -n5 | sed -e 's/.*\s//g' | sed ':a;N;$!ba;s/\n/, /g')"

# Write script.js
cat >./script.js <<EOF
	var ctx = document.getElementById("myChart").getContext('2d');
	var myChart = new Chart(ctx, {
	    type: 'bar',
	    data: {
	        labels: [$Labels],  // ["Red", "Blue", "Yellow", "Green", "Purple"],
	        datasets: [{
	            label: '# of views per video',
	            data: [$Data],  // [12, 19, 3, 5, 2],
	            backgroundColor: [
	                'rgba(255, 99, 132, 0.2)',
	                'rgba(54, 162, 235, 0.2)',
	                'rgba(255, 206, 86, 0.2)',
	                'rgba(75, 192, 192, 0.2)',
	                'rgba(153, 102, 255, 0.2)',
	            ],
	            borderColor: [
	                'rgba(255,99,132,1)',
	                'rgba(54, 162, 235, 1)',
	                'rgba(255, 206, 86, 1)',
	                'rgba(75, 192, 192, 1)',
	                'rgba(153, 102, 255, 1)',
	            ],
	            borderWidth: 1
	        }]
	    },
	    options: {
		maintainAspectRatio: false,
	        scales: {
	            yAxes: [{
	                ticks: {
	                    beginAtZero:true
	                }
	            }]
	        }
	    }
	});
EOF
fi
