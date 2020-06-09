package cs1.cb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PairMapStripeReduceRelativeTest {
	MapDriver<LongWritable, Text, PairTextWritable, IntWritable> mapDriver;
	ReduceDriver<PairTextWritable, IntWritable, Text, ExtendMapWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, PairTextWritable, IntWritable, Text, ExtendMapWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		PairMapStripeReduceRelative.Map mapper = new PairMapStripeReduceRelative.Map();
		PairMapStripeReduceRelative.Reduce reducer = new PairMapStripeReduceRelative.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("a b c d"));
		mapDriver.withOutput(new PairTextWritable("a", "b"), new IntWritable(1))
			.withOutput(new PairTextWritable("a", "c"), new IntWritable(1))
			.withOutput(new PairTextWritable("a", "d"), new IntWritable(1))
			.withOutput(new PairTextWritable("b", "c"), new IntWritable(1))
			.withOutput(new PairTextWritable("b", "d"), new IntWritable(1))
			.withOutput(new PairTextWritable("c", "d"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		ExtendMapWritable emw = new ExtendMapWritable();
		emw.put(new Text("b"), new DoubleWritable(1.0/2));
		emw.put(new Text("c"), new DoubleWritable(1.0/2));
		reduceDriver.withInput(new PairTextWritable("a", "b"), values)
			.withInput(new PairTextWritable("a", "c"), values);
		reduceDriver.withOutput(new Text("a"), emw);
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		ExtendMapWritable emwa = new ExtendMapWritable();
		emwa.put(new Text("b"), new DoubleWritable(1.0/2));
		emwa.put(new Text("c"), new DoubleWritable(1.0/2));
		ExtendMapWritable emwb = new ExtendMapWritable();
		emwb.put(new Text("c"), new DoubleWritable(1.0));
		
		mapReduceDriver.withInput(new LongWritable(), new Text("a b c"));
		mapReduceDriver.withOutput(new Text("a"), emwa)
			.withOutput(new Text("b"), emwb);
		mapReduceDriver.runTest();
	}
}
