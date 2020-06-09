package cs1.wc;

import java.io.IOException;
import java.util.*;
     
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
     
public class AverageQuantity {
     
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	     
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] words = line.split(" ");
	        String sQuantity = words[words.length - 1];
	        if (!sQuantity.equals("-")) {
		        String ip = words[0];
		        int quantity = Integer.parseInt(sQuantity);
		        context.write(new Text(ip), new IntWritable(quantity));
	        }
	    }
	}
     
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	 
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
	        int sum = 0, count = 0;
		    for (IntWritable val : values) {
		        sum += val.get();
		        count++;
		    }
		    context.write(key, new IntWritable(sum/count));
	    }
	}
     
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	     
	    Job job = new Job(conf, "wordcount");
	    job.setJarByClass(AverageQuantity.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	     
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	     
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	    job.waitForCompletion(true);
	}
     
}