package edu.itu.se.csc550.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  
 *
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */
public class CustomKeySortComparator extends WritableComparator {

		  protected CustomKeySortComparator() {
				super(CustomeKeyWritable.class, true);
			}

			@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				int cmpResult = 0;
				if(w1 instanceof CustomeKeyWritable){
				CustomeKeyWritable key1 = (CustomeKeyWritable) w1;
				CustomeKeyWritable key2 = (CustomeKeyWritable) w2;
				cmpResult = key1.getKey().compareTo(key2.getKey());
				} else if(w1 instanceof LongWritable){	
				cmpResult = w1.compareTo(w2);		
				}
				return cmpResult;
			}
		
	
}
