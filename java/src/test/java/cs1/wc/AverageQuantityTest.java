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

public class AverageQuantityTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		AverageQuantity.Map mapper = new AverageQuantity.Map();
		AverageQuantity.Reduce reducer = new AverageQuantity.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("10.0.0.1 20"))
			.withInput(new LongWritable(), new Text("10.0.0.2 30"))
			.withInput(new LongWritable(), new Text("10.0.0.1 10"));
		mapDriver.withOutput(new Text("10.0.0.1"), new IntWritable(20))
			.withOutput(new Text("10.0.0.2"), new IntWritable(30))
			.withOutput(new Text("10.0.0.1"), new IntWritable(10));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(20));
		values.add(new IntWritable(10));
		reduceDriver.withInput(new Text("10.0.0.1"), values);
		reduceDriver.withOutput(new Text("10.0.0.1"), new IntWritable(15));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("10.0.0.1 20"))
			.withInput(new LongWritable(), new Text("10.0.0.2 30"))
			.withInput(new LongWritable(), new Text("10.0.0.1 10"));
		mapReduceDriver.withOutput(new Text("10.0.0.1"), new IntWritable(15))
			.withOutput(new Text("10.0.0.2"), new IntWritable(30));
		mapReduceDriver.runTest();
	}
}
