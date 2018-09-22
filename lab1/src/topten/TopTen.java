package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>(Collections.reverseOrder());

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    
		Map<String, String> xmlToMap = transformXmlToMap(value.toString());
            
            String userId = xmlToMap.get("Id");
            String reputation = xmlToMap.get("Reputation");
            
			//Discard the invalid row
            if (userId != null)
                repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
	}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			for(Map.Entry<Integer,Text> entry : repToRecordMap.entrySet()) {
				Text entryText = new Text(entry.getValue());
				context.write(NullWritable.get(), entryText);
			}
		}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Integer> repToRecordMap = new TreeMap<Integer, Integer>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				Map<String, String> xmlToMap = transformXmlToMap(value.toString());
				repToRecordMap.put(Integer.parseInt(xmlToMap.get("Reputation")), Integer.parseInt(xmlToMap.get("Id")));

				// If we have more than ten records, remove the one with the lowest reputation
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			for (Map.Entry<Integer, Integer> entry : repToRecordMap.entrySet()) {
				Put insHBase = new Put(Bytes.toBytes(entry.getValue()));

				insHBase.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(entry.getValue()));
				insHBase.add(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(entry.getKey()));

				context.write(null, insHBase);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TopTen.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
