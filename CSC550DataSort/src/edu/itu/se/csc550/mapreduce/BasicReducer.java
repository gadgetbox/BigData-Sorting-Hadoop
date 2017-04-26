package edu.itu.se.csc550.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */

public class BasicReducer extends Reducer<CustomeKeyWritable, Text, CustomeKeyWritable, NullWritable> {

	@Override
	public void reduce(CustomeKeyWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		LongWritable k2 = new LongWritable(key.getKey());
		for ( Text value : values) {		
			//context.write(key, new Text());
			context.write(key, NullWritable.get());
		}
	
	}

}
