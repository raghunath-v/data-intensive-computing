package project;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Program to find the most popular video category in youtube.

public class Viral 
{
	// Mapper class that has a function to output {(Category1, 1),...., (CategoryN, 1))
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		private Text cat = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String row = value.toString();
			String data[]=row.split("\t");

			if(data.length > 5)
			{
				cat.set(data[3]);
			}
			context.write(cat, one);
		}
	}

	// Reducer class that has a function to sum up the values
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
	    		int sum = 0;
	    		for (IntWritable val : values) {
	    		    sum += val.get();
	    		}
      
	    		result.set(sum);
	    		context.write(key, result);
		}
    	}

	// Driver
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Viral.class);
    
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
    
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}
}

