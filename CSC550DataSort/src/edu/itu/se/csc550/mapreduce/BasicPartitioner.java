package edu.itu.se.csc550.mapreduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  
 *
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */

public class BasicPartitioner  extends
	  	Partitioner<CustomeKeyWritable, Text> {
	
		public static int MAX_LIMIT = 1000;
	
		@Override
		public int getPartition(CustomeKeyWritable key, Text value,
				int noOfReducers) {

			if(noOfReducers > 1) {
			
			int interval = (int) MAX_LIMIT / noOfReducers;
			
			int partition = ((int) ( key.getKey() % interval));
			
			return partition % noOfReducers ;
			
			}else {
				return 0;
			}
			
		}
		
}
