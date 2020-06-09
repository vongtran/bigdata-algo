package cs1.wc;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
     
public class InMapperWordCount {
     
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private static HashMap<String, Integer> h;
    @Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
    	for (Entry<String, Integer> e : h.entrySet()) {
    		context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
    	}
		super.cleanup(context);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		h = new HashMap<String, Integer>();
		super.setup(context);
	}

	private Text word = new Text();
     
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
	    	String s = tokenizer.nextToken();
	        if (h.containsKey(s)) {
	        	int v = h.get(s);
	        	h.put(s, v+1);
	        } else {
	        	h.put(s, 1);
	        }
	    }
    }
 }
     
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        int sum = 0;
    for (IntWritable val : values) {
        sum += val.get();
    }
    context.write(key, new IntWritable(sum));
    }
 }
     
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
     
    Job job = new Job(conf, "wordcount");
    job.setJarByClass(InMapperWordCount.class);
     
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