package cs1.wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		WordCount.Map mapper = new WordCount.Map();
		WordCount.Reduce reducer = new WordCount.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("andy tony andy"));
		mapDriver.withOutput(new Text("andy"), new IntWritable(1)).withOutput(new Text("tony"), new IntWritable(1))
			.withOutput(new Text("andy"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> andyValues = new ArrayList<IntWritable>();
		andyValues.add(new IntWritable(1));
		andyValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("andy"), andyValues);
		reduceDriver.withOutput(new Text("andy"), new IntWritable(2));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("andy tony andy"));
		mapReduceDriver.withOutput(new Text("andy"), new IntWritable(2)).withOutput(new Text("tony"), new IntWritable(1));
		mapReduceDriver.runTest();
	}
}
