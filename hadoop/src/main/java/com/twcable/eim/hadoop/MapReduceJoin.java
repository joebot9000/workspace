package com.twcable.eim.hadoop;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduceJoin extends Configured{
	
	public static class MapReduceJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
	
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String row = value.toString();
			String[] tokens = row.split(",");
			String myKey = tokens[0];
			String myValue = fileSplit.getPath().getName() + "," + row;
					
			outputKey.set(myKey);			
			outputValue.set(myValue);
			
			context.write(outputKey, outputValue);
		}
	}
	
	  public static class MapReduceJoinReducer extends Reducer<Text, Text,Text, Text> {

		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  Text myKey = new Text();
			  Text myValue = new Text();
			  
			  String leftSideValue = null;
			  String rightSideValue = null;
			  
		      for (Text val : values) {
		    	if(val.toString().contains("cust"))
		    	{
		    		leftSideValue = val.toString();
		    	}
		    	
		    	if(val.toString().contains("tran"))
		    	{
		    		rightSideValue = val.toString();
		    	}
		    	
		    	myKey.set(key);
		    	myValue.set(leftSideValue + "," + rightSideValue);
		    	
		    	context.write(myKey, myValue);
		      }
		      

		    }
		  }



public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", ",");
	    conf.setBoolean("mapred.output.compress",true);
	    conf.setBoolean("mapred.intermediate.compress",true);
	    conf.set("mapred.output.compression.type", "BLOCK");   
	    conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
	    
	    Job job = Job.getInstance(conf, "mapreduce join");
	    job.setJarByClass(MapReduceJoin.class);
	    
	    job.setMapperClass(MapReduceJoinMapper.class);
	    job.setCombinerClass(MapReduceJoinReducer.class);
	    job.setReducerClass(MapReduceJoinReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	   
	    System.out.println("Input File 1: " + args[0]);
	    System.out.println("Input File 2: " + args[1]);
	    System.out.println("Output File: " + args[2]);
	    
	    job.waitForCompletion(true);

 }
}
