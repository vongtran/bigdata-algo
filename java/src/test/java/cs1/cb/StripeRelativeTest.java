package cs1.cb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class StripeRelativeTest {
	MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
	ReduceDriver<Text, MapWritable, Text, ExtendMapWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, ExtendMapWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		StripeRelative.Map mapper = new StripeRelative.Map();
		StripeRelative.Reduce reducer = new StripeRelative.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		MapWritable mapa = new MapWritable();
		mapa.put(new Text("b"), new IntWritable(1));
		mapa.put(new Text("c"), new IntWritable(1));
		MapWritable mapb = new MapWritable();
		mapb.put(new Text("c"), new IntWritable(1));
		mapDriver.withInput(new LongWritable(), new Text("a b c"));
		mapDriver.withOutput(new Text("a"), mapa)
			.withOutput(new Text("b"), mapb);
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<MapWritable> values = new ArrayList<MapWritable>();
		MapWritable mapa1 = new MapWritable();
		mapa1.put(new Text("b"), new IntWritable(1));
		mapa1.put(new Text("c"), new IntWritable(1));
		MapWritable mapa2 = new MapWritable();
		mapa2.put(new Text("b"), new IntWritable(1));
		values.add(mapa1);
		values.add(mapa2);
		ExtendMapWritable emw = new ExtendMapWritable();
		emw.put(new Text("b"), new DoubleWritable(2.0/3));
		emw.put(new Text("c"), new DoubleWritable(1.0/3));
		reduceDriver.withInput(new Text("a"), values);
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
