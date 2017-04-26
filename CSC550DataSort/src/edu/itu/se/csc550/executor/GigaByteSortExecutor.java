package edu.itu.se.csc550.executor;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import edu.itu.se.csc550.mapreduce.BasicInputFormat;
import edu.itu.se.csc550.mapreduce.BasicPartitioner;
import edu.itu.se.csc550.mapreduce.BasicReducer;
import edu.itu.se.csc550.mapreduce.CustomKeySortComparator;
import edu.itu.se.csc550.mapreduce.CustomeKeyWritable;
import edu.itu.se.csc550.mapreduce.BasicMapper;

/**
 *  
 *
 * @author gaurav-sjsu @Date 26-04-2016
 *
 */
public class GigaByteSortExecutor extends Configured implements Tool {

		  @Override
			public int run(String[] args) throws Exception {

				if (args.length != 3) {
					System.out
							.printf("Three parameters are required for GigaByteSort- <input dir> <output dir> <partitioning path> \n");
					return -1;
				}

				Job job = new Job(getConf());
				job.setJobName("GigaByteSort");
				
				System.out.println(" Executing with Config: ");
				System.out.println(" No of Reducers :  "+job.getNumReduceTasks());
			
				job.setJarByClass(getClass());				
				//job.setNumReduceTasks(8);

				FileInputFormat.setInputPaths(job, new Path(args[0]));
				Path partitionOutputPath = new Path(args[2]);
				FileOutputFormat.setOutputPath(job, new Path(args[1]));				
					
				job.setMapperClass(BasicMapper.class);
				job.setMapOutputKeyClass(CustomeKeyWritable.class);
				job.setMapOutputValueClass(Text.class);
				job.setSortComparatorClass(CustomKeySortComparator.class);
				
				TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
				job.setInputFormatClass(BasicInputFormat.class);

				//Write partition file with random sampler
				InputSampler.Sampler<CustomeKeyWritable, Text> sampler = 
		        		new InputSampler.RandomSampler<CustomeKeyWritable, Text>(0.01, 1000, 100);
				InputSampler.writePartitionFile(job, sampler);
		       
				job.setPartitionerClass(TotalOrderPartitioner.class);
		        //job.setPartitionerClass(BasicPartitioner.class);

				
				job.setReducerClass(BasicReducer.class);
				job.setOutputKeyClass(CustomeKeyWritable.class);
				job.setOutputValueClass(NullWritable.class);
				
				boolean success = job.waitForCompletion(true);
				edu.itu.se.csc550.service.BasicMergeOutput.mergeReducerOutputFiles(getConf(), new Path(args[1])+"", new Path(args[1])+File.separator+"all.txt");
				return success ? 0 : 1;
			}

			
		  public static void main(String[] args) throws Exception {			  
			  //System.out.println(" ARGS >> "+args.length);
			  //args = new String[3];
			  //args[0] = "/home/dev1/tools/RandomNumbersSmall-KV.txt";
			  //args[1] = "/home/dev1/tools/output-secondarySortBasic";
			  System.out.println("... Executing GigaByteSortExecutor ...");
			  long start = System.currentTimeMillis();	
				int exitCode = ToolRunner.run(new Configuration(),
						new GigaByteSortExecutor(), args);
				long end = System.currentTimeMillis();
				System.out.println("... GigaByteSortExecutor Completed ...");
				System.out.println("Total time taken is "+((end-start)/1000)+" Seconds. ");

			  System.exit(exitCode);
			}		

}
