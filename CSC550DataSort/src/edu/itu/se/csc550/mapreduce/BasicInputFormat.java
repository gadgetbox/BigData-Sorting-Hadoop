package edu.itu.se.csc550.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *  
 *
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */

public class BasicInputFormat extends FileInputFormat<CustomeKeyWritable,Text> {
	
		@Override
	    public RecordReader<CustomeKeyWritable,Text> 
		createRecordReader(InputSplit split, TaskAttemptContext context) 
				throws IOException, InterruptedException {
	        return new BasicRecordReader();
	    }

	
}
