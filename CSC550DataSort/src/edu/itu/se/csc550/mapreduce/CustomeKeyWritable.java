package edu.itu.se.csc550.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *  
 *
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */

public class CustomeKeyWritable implements Writable, 
Comparable<CustomeKeyWritable>, WritableComparable<CustomeKeyWritable> {
	

	private Integer index;
	private Integer key;

	public CustomeKeyWritable() {
	}

	public CustomeKeyWritable(Integer index, Integer key) {
		this.index = index;
		this.key = key;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(index).append("\t")
				.append(key)).toString();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.index = dataInput.readInt(); //WritableUtils. .readVInt(dataInput); // .readInteger(dataInput);
		this.key = dataInput.readInt(); //WritableUtils.readVInt(dataInput);// .readInteger(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(index);
		dataOutput.writeInt(key);
		//WritableUtils.writeVInt(dataOutput, index); //.writeInteger(dataOutput, index);
		//WritableUtils.writeVInt(dataOutput, key); //writeInteger(dataOutput, key);
		
	}

	public int compareTo(CustomeKeyWritable objKeyPair) {
		
		int result = this.key.compareTo(objKeyPair.getKey());
		return result;
	}

	public Integer getIndex() {
		return index;
	}

	public void setIndex(Integer index) {
		this.index = index;
	}

	public Integer getKey() {
		return key;
	}

	public void setKey(Integer key) {
		this.key = key;
	}

}
