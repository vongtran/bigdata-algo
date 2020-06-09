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

public class PairRelativeTest {
	MapDriver<LongWritable, Text, PairTextWritable, IntWritable> mapDriver;
	ReduceDriver<PairTextWritable, IntWritable, PairTextWritable, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, PairTextWritable, IntWritable, PairTextWritable, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		PairRelative.Map mapper = new PairRelative.Map();
		PairRelative.Reduce reducer = new PairRelative.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("a b c"));
		mapDriver.withOutput(new PairTextWritable("a", "b"), new IntWritable(1))
			.withOutput(new PairTextWritable("a", "*"), new IntWritable(1))
			.withOutput(new PairTextWritable("a", "c"), new IntWritable(1))
			.withOutput(new PairTextWritable("a", "*"), new IntWritable(1))
			.withOutput(new PairTextWritable("b", "c"), new IntWritable(1))
			.withOutput(new PairTextWritable("b", "*"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		List<IntWritable> values1 = new ArrayList<IntWritable>();
		values1.add(new IntWritable(1));
		reduceDriver.withInput(new PairTextWritable("a", "*"), values)
			.withInput(new PairTextWritable("a", "b"), values1)
			.withInput(new PairTextWritable("a", "c"), values1);
		reduceDriver.withOutput(new PairTextWritable("a", "b"), new DoubleWritable(1.0/2))
			.withOutput(new PairTextWritable("a", "c"), new DoubleWritable(1.0/2));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		List<IntWritable> values1 = new ArrayList<IntWritable>();
		values1.add(new IntWritable(1));
		ExtendMapWritable emwa = new ExtendMapWritable();
		emwa.put(new Text("b"), new DoubleWritable(1.0/2));
		emwa.put(new Text("c"), new DoubleWritable(1.0/2));
		ExtendMapWritable emwb = new ExtendMapWritable();
		emwb.put(new Text("c"), new DoubleWritable(1.0));
		
		mapReduceDriver.withInput(new LongWritable(), new Text("a b c"));
		mapReduceDriver.withOutput(new PairTextWritable("a", "b"), new DoubleWritable(1.0/2))
		.withOutput(new PairTextWritable("a", "c"), new DoubleWritable(1.0/2))
		.withOutput(new PairTextWritable("b", "c"), new DoubleWritable(1.0));
		mapReduceDriver.runTest();
	}
}
