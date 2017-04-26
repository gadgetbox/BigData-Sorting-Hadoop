package edu.itu.se.csc550.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

/**
 *  
 *
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */

public class BasicRecordReader extends RecordReader<CustomeKeyWritable, Text>  {
 
    private LineRecordReader in;
    private CustomeKeyWritable key = new CustomeKeyWritable();
    private Text value;
 
    @Override
    public void close() throws IOException {
        if (null != in) {
            in.close();
            in = null;
        }
        key = null;
        value = null;
    }

    @Override
    public CustomeKeyWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return in.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        close();

        in = new LineRecordReader();
        in.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!in.nextKeyValue()) {
            key = null;
            value = null;
            return false;
        }
        key = new CustomeKeyWritable();
        value = new Text();
        Text line = in.getCurrentValue();
        //System.out.println(" LINE >> "+line.toString());
        String str = line.toString();
        String[] arr = str.split("\t");
        //System.out.println("arr length "+arr.length);
        //System.out.println("at 0 "+(arr[0]));
        //System.out.println("at 1 "+(arr[1]));
        key.setIndex(Integer.parseInt((arr[0])));
        key.setKey(Integer.parseInt((arr[1])));
        value = new Text(arr[1]);
        return true;
    }


}
