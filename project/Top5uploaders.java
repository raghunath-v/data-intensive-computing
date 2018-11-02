package project;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Program to find the video category with most average views per video in youtube.

public class Top5uploaders
{
	// Mapper class that has a function to output {(Category1, 1),...., (CategoryN, 1))
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
	{
		private final static FloatWritable one = new FloatWritable(1.0f);
		private  FloatWritable rating = new FloatWritable();
		private  FloatWritable viewrate = new FloatWritable();
		private  FloatWritable views = new FloatWritable();
		private Text cat = new Text();
		private Text uploader = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String row = value.toString();
			String data[]=row.split("\t");

			if(data.length > 5)
			{
				cat.set(data[3]);
				uploader.set(data[1]);
				float view_count = 0;
				float time = 0;
				
				if(data[2].matches("\\d+.+") && data[5].matches("\\d+.+"))
				{
					view_count = Float.parseFloat(data[5]);
					time = Float.parseFloat(data[2]);
				}
				viewrate.set(view_count/time);
				views.set(view_count);

				if(data[6].matches("\\d+.+"))
				{
					float f=Float.parseFloat(data[6]);
                 			rating.set(f);
				}
			}
			context.write(uploader, one);
		}
	}

	// Reducer class that has a function to sum up the values
	public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
	{
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException 
		{
	    		float sum = 0;
			int iter = 0;
	    		for (FloatWritable val : values) {
	    		    sum += val.get();
			    iter++;
	    		}
     			result .set(sum); 
	    		//result.set(sum/iter);
	    		context.write(key, result);
		}
    	}

	// Driver
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Top5uploaders.class);
    
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
    
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}
}

