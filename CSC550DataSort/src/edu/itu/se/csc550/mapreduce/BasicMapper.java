package edu.itu.se.csc550.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */
public class BasicMapper extends Mapper<CustomeKeyWritable, Text, CustomeKeyWritable, Text> {
	CustomeKeyWritable in = new CustomeKeyWritable();
    
    @Override
	public void map(CustomeKeyWritable key, Text value, Context context)
			throws IOException, InterruptedException {
    	String line = value.toString();
		//System.out.println("input LINE "+line);
    	if (line.toString().length() > 0) {
			String arrDataAttributes[] = line.toString().split(" ");
			//System.out.println("Value >> "+value);
			/*in = new CustomeKeyWritable(
					Integer.parseInt(arrDataAttributes[0].toString().trim()),
					Integer.parseInt(arrDataAttributes[1].toString().trim()));*/
			value = new Text(arrDataAttributes[0].toString().trim());
			
			context.write( key, value );
		//}
	}
    }
}
