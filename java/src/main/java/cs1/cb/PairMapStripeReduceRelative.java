package cs1.cb;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
     
public class PairMapStripeReduceRelative {
     
	public static class Map extends Mapper<LongWritable, Text, PairTextWritable, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer st = new StringTokenizer(line);
	        List<String> words = new ArrayList<String>();
	        while (st.hasMoreTokens()) {
	        	words.add(st.nextToken());
	        }
	        for (int i = 0; i < words.size()-1; i++) {
	        	for (int j = i+1; j < words.size() && (!words.get(i).equals(words.get(j))); j++) {
	        		context.write(new PairTextWritable(words.get(i), words.get(j)), one);
	        	}
	        }

	    }
	}
     
 public static class Reduce extends Reducer<PairTextWritable, IntWritable, Text, ExtendMapWritable> {
	private HashMap<String, Integer> h;
	private Text kprev;
	@Override
	protected void setup(Reducer<PairTextWritable, IntWritable, Text, ExtendMapWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		h = new HashMap<String, Integer>();
		kprev = new Text();
	}
	
	@Override
	protected void cleanup(Reducer<PairTextWritable, IntWritable, Text, ExtendMapWritable>.Context context)
			throws IOException, InterruptedException {
//		int total = 0;
//		for (Entry<String, Integer> e : h.entrySet()) {
//    		total += e.getValue();
//    	}
//		ExtendMapWritable mw = new ExtendMapWritable();
//    	for (Entry<String, Integer> e : h.entrySet()) {
//    		mw.put(new Text(e.getKey()), new DoubleWritable(1.0*e.getValue()/total));
//    	}
//    	context.write(kprev, mw);
		emit(context);
		super.cleanup(context);
	}

	public void reduce(PairTextWritable key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        if (!kprev.toString().isEmpty() && !kprev.toString().equals(key.getLeft().toString())) {
//    		ExtendMapWritable mw = new ExtendMapWritable();
//    		int total = 0;
//    		for (Entry<String, Integer> e : h.entrySet()) {
//        		total += e.getValue();
//        	}
//        	for (Entry<String, Integer> e : h.entrySet()) {
//        		mw.put(new Text(e.getKey()), new DoubleWritable(1.0*e.getValue()/total));
//        	}
//        	context.write(kprev, mw);
        	emit(context);
        	h = new HashMap<String, Integer>();
    	} 
		int sum = 0;
        for (IntWritable val : values) {
	    	sum += val.get();
	    }
        String str = key.getRight().toString();
    	if (h.containsKey(str)) {
    		int v = h.get(str);
    		h.put(str, v+sum);
    	} else {
    		h.put(str, sum);
    	}
        kprev.set(key.getLeft());
	 }
	
	private void emit(Reducer<PairTextWritable, IntWritable, Text, ExtendMapWritable>.Context context)
			throws IOException, InterruptedException {
		int total = 0;
		for (Entry<String, Integer> e : h.entrySet()) {
    		total += e.getValue();
    	}
		ExtendMapWritable mw = new ExtendMapWritable();
    	for (Entry<String, Integer> e : h.entrySet()) {
    		mw.put(new Text(e.getKey()), new DoubleWritable(1.0*e.getValue()/total));
    	}
    	context.write(kprev, mw);
	}
 }
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
     
    Job job = new Job(conf, "wordcount");
    job.setJarByClass(PairMapStripeReduceRelative.class);
     
    job.setMapOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(PairTextWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ExtendMapWritable.class);
     
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
     
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
     
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
     
    job.waitForCompletion(true);
 }
     
}