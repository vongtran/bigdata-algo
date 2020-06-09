package cs1.cb;

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
     
public class StripeRelative {
     
 public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
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
        	HashMap<String, Integer> h = new HashMap<String, Integer>();
        	for (int j = i+1; j < words.size() && (!words.get(i).equals(words.get(j))); j++) {
        		if (h.containsKey(words.get(j))) {
        			Integer v = h.get(words.get(j));
        			h.put(words.get(j), v+1);
        		} else {
        			h.put(words.get(j), 1);
        		}
        	}
        	MapWritable mw = new MapWritable();
        	for (Entry<String, Integer> e : h.entrySet()) {
        		mw.put(new Text(e.getKey()), new IntWritable(e.getValue()));
        	}
        	context.write(new Text(words.get(i)), mw);
        }
    }
 }
     
 public static class Reduce extends Reducer<Text, MapWritable, Text, ExtendMapWritable> {
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
      throws IOException, InterruptedException {
        int sum = 0;
        HashMap<String, Double> hf = new HashMap<String, Double>();
	    for (MapWritable val : values) {
	    	for (Entry<Writable, Writable> e : val.entrySet()) {
	    		String k = ((Text)e.getKey()).toString();
	    		Integer v = ((IntWritable)e.getValue()).get();
	    		sum += v;
	    		if (hf.containsKey(k)) {
	    			double newValue = hf.get(k) + v;
	    			hf.put(k, newValue);
	    		} else {
	    			hf.put(k, 1.0*v);
	    		}
	    	}
	    }
	    for (Entry<String, Double> e : hf.entrySet()) {
    		double v = e.getValue()/sum;
    		hf.put(e.getKey(), v);
    	}
	    ExtendMapWritable mw = new ExtendMapWritable();
    	for (Entry<String, Double> e : hf.entrySet()) {
    		mw.put(new Text(e.getKey()), new DoubleWritable(e.getValue()));
    	}
    	context.write(key, mw);
	 }
 }
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
     
    Job job = new Job(conf, "wordcount");
    job.setJarByClass(StripeRelative.class);
     
    job.setMapOutputValueClass(MapWritable.class);
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