package edu.itu.se.csc550.service;
//Example Driver class for Word count program
import java.io.*;
import java.util.Random;

import org.apache.hadoop.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RandomDataGenerator2 
{
	public static void  main(String [] args) throws FileNotFoundException, UnsupportedEncodingException {
		args = new String[1];
		args[0] = "/home/dev1/Downloads/";
		
		if(args.length != 1){
			System.out.println("No Output Dir provided.");
			System.out.println(" type : GigaByteDataGeneratorExecutor <outputDirPathForFile>");
			return;
		}
		
		String fileName = "InputRandomData.txt";
		String fileEncoding = "UTF-8";
		
		PrintWriter writer = new PrintWriter(args[0]+File.separator+fileName, fileEncoding );
		Random rnd = new Random();
	
		
		double n =0;
		System.out.println("Started");
		 for(double i=0;i<599555000;i++) {  			// Current value of i creates 13 GB data with K,V pair
			n = rnd.nextInt(1000 * 1000 )+1; 				// Use i=995000, 999995000 to create 10 MB, 10GB file
			writer.println((int)i+1+" "+(int)n);    // Random Numbers are created between 1 to 1M
		 }		
		 
		writer.close();
		System.out.println("end");
		return;
	}
}