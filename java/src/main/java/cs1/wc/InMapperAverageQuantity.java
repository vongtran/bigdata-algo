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
     
public class InMapperAverageQuantity {
     
 public static class Map extends Mapper<LongWritable, Text, Text, PairIntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private HashMap<String, Pair<Integer, Integer>> h; 
    @Override
	protected void cleanup(Mapper<LongWritable, Text, Text, PairIntWritable>.Context context)
			throws IOException, InterruptedException {
		for (Entry<String, Pair<Integer, Integer>> e : h.entrySet()) {
			String ip = e.getKey();
			Pair<Integer, Integer> p = e.getValue();
			context.write(new Text(ip), new PairIntWritable(new IntWritable(p.getLeft()), new IntWritable(p.getRight())));
		}
		super.cleanup(context);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, PairIntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		h = new HashMap<String, Pair<Integer,Integer>>();
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        String sQuantity = words[words.length - 1];
        if (!sQuantity.equals("-")) {
	        String ip = words[0];
	        int quantity = Integer.parseInt(sQuantity);
	        if (h.containsKey(ip)) {
	        	Pair<Integer, Integer> v = h.get(ip);
	        	Integer sum = v.getLeft();
	        	Integer count = v.getRight();
	        	v.setLeft(sum+quantity);
	        	v.setRight(count+1);
	        	h.put(ip, v);
	        } else {
	        	Pair<Integer, Integer> v = new Pair<Integer, Integer>(quantity, 1);
	        	h.put(ip, v);
	        }
        }
    }
 }
     
 public static class Reduce extends Reducer<Text, PairIntWritable, Text, IntWritable> {
 
    public void reduce(Text key, Iterable<PairIntWritable> values, Context context)
      throws IOException, InterruptedException {
        int sum = 0, count = 0;
    for (PairIntWritable val : values) {
        sum += val.getLeft().get();
        count += val.getRight().get();
    }
    context.write(key, new IntWritable(sum/count));
    }
 }
     
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
     
    Job job = new Job(conf, "wordcount");
    job.setJarByClass(InMapperAverageQuantity.class);
    job.setMapOutputValueClass(PairIntWritable.class); 
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