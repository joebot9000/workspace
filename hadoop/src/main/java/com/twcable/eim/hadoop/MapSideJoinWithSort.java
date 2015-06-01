package com.twcable.eim.hadoop;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapSideJoinWithSort extends Configured{

	private static String leftFileName;
	private static String leftFileDelimiter;
	private static boolean leftFileCached;
	
	private static String rightFileName;
	private static String rightFileDelimiter;
	private static boolean rightFileCached;
	
	private static String outputFileName;
	private static String outputFileDelimiter;
	
	private static String jobName;
	private static String joinType;

	
	public static class MapSideJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Map<String, String> leftCachedData = new HashMap<String, String>();
		private Map<String, String> rightCachedData = new HashMap<String, String>();
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
	
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			try {
			
				if (context.getCacheFiles() != null) 
				{
					URI[] cacheFiles = context.getCacheFiles();
					if (cacheFiles != null) 
					{
						for (URI cachedFile : cacheFiles) {
							System.out.println("Cached File Found: " + cachedFile);
							if (cachedFile != null) 
							{
								File f = new File(cachedFile.getPath());
								System.out.println("Cached file match found");
								System.out.println("Cached file path: " + f.getAbsolutePath());
								System.out.println("Cached file name: " + f.getName());
								
								String lFileName = context.getConfiguration().get("leftFileName");
								String rFileName = context.getConfiguration().get("rightFileName");
								
								if(lFileName.toString().contains(f.getName()))
								{
									System.out.println("leftFileName: " + lFileName);
									loadCachedFile(f, true, context);
								}
								
								if(rFileName.toString().contains(f.getName()))
								{
									System.out.println("rightFileName: " + rFileName);
									loadCachedFile(f, false, context);
								}
								
							}

						}
					} 
					else 
					{
						System.out.println("Cached files is null");
					}

				} 
				else 
				{
					System.out.println("Get Cached files is null");
				}
			} 
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private void loadCachedFile(File f, Boolean leftFile, Context context) throws FileNotFoundException, IOException, Exception {
			BufferedReader reader = new BufferedReader(new FileReader("./" + f.getName()));
			String line = reader.readLine();
			String fileDelimiter;
			Map<String, String> cachedData = new HashMap<String, String>();
			
			if(leftFile)
			{
				fileDelimiter = context.getConfiguration().get("leftFileDelimiter");
			}
			else
			{
				fileDelimiter = context.getConfiguration().get("rightFileDelimiter");
			}
			
			while (line != null) 
			{
				String[] columns = line.split(fileDelimiter);
				String myKey = columns[0];
				cachedData.put(myKey, line);
				line = reader.readLine();
			}
			
			if(leftFile)
			{
				leftCachedData = cachedData;
			}
			else
			{
				rightCachedData = cachedData;
			}

			System.out.println("customerData Loaded with: " + cachedData.size());

			reader.close();

			if (cachedData.size() == 0) 
			{
				throw new Exception("Cached Data Not Loaded!");
			}
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try{
				
			leftFileDelimiter = context.getConfiguration().get("leftFileDelimiter");
			rightFileDelimiter = context.getConfiguration().get("rightFileDelimiter");
			outputFileDelimiter = context.getConfiguration().get("outputFileDelimiter");
			
			leftFileCached = context.getConfiguration().getBoolean("leftFileCached", false);
			rightFileCached = context.getConfiguration().getBoolean("rightFileCached", false);
			
			joinType = context.getConfiguration().get("joinType");
			
			//take the incoming row of data.  this will be based on the input format split.  Most likely this will be new line char.
			String row = value.toString();
			
			if(leftFileCached && rightFileCached)
			{
					// this is a join between two cached files

					if (joinType == "left") {
						for (String myKey : leftCachedData.keySet()) {
							outputKey.set(myKey.trim());
							outputValue.set(rightCachedData.get(myKey));
							context.write(outputKey, outputValue);
						}
						
					}

					if (joinType == "right") {
						for (String myKey : rightCachedData.keySet()) {
							outputKey.set(myKey.trim());
							outputValue.set(leftCachedData.get(myKey));
							context.write(outputKey, outputValue);
						}
					
					}
					
					if (joinType == "inner") {
						for (String myKey : rightCachedData.keySet()) {
							outputKey.set(myKey.trim());
							outputValue.set(leftCachedData.get(myKey));
							
							if(outputValue.toString() != "null")
							{
								context.write(outputKey, outputValue);
							}
						}
						
					}


			}
			
			if(leftFileCached && !rightFileCached)
			{
				//split the row based on the specified delimiter
				String[] columns = row.split(leftFileDelimiter);
				
				//assume the key is the first column
				String myKey = columns[0];
				
				//this is the join, take the original row from the left file and attach the row found in the right file.
				String myValue = row.replace(myKey + leftFileDelimiter , "") + leftCachedData.get(myKey).replace(myKey, "").trim().replace(leftFileDelimiter, outputFileDelimiter);;
				
				outputKey.set(myKey);
				
				if(joinType.equals("left"))
				{
					outputValue.set(myValue);
					context.write(outputKey, outputValue);	
				}
				
				if(joinType.equals("inner") && !myValue.toString().contains("null"))
				{
					outputValue.set(myValue);
					context.write(outputKey, outputValue);	
				}
			}
			
			if(!leftFileCached && rightFileCached)
			{
				//split the row based on the specified delimiter
				String[] columns = row.split(rightFileDelimiter);
				
				//assume the key is the first column
				String myKey = columns[0];
				
				//this is the join, take the original row from the left file and attach the row found in the right file.
				String myValue = row.replace(myKey + rightFileDelimiter , "") + rightCachedData.get(myKey).replace(myKey, "").trim().replace(rightFileDelimiter, outputFileDelimiter);

				outputKey.set(myKey);
				
				if(joinType.equals("left"))
				{
					outputValue.set(myValue);
					context.write(outputKey, outputValue);	
				}
				
				if(joinType.equals("inner") && !myValue.toString().contains("null"))
				{
					outputValue.set(myValue);
					context.write(outputKey, outputValue);	
				}				
			}

			if(!leftFileCached && !rightFileCached)
			{
				//this cannot be mapside join, please use reducer.
				throw new Exception("This cannot be mapside join, please use reducer or put one file into cache");
			}
			}
			catch(Exception e){
				e.getStackTrace();
			}
			
		}
	}


	  public static class MapSideJoinReducer extends Reducer<Text, Text,Text, Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		      for (Text val : values) {    	
		    	context.write(key, val);
		      }	    
		    }
		  }

public static void main(String[] args) throws Exception {

		for(String arg : args)
		{
			System.out.println(arg);
		}
		
    	Configuration conf = new Configuration();
	    conf.set("mapreduce.output.textoutputformat.separator", ",");
	    conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
	    conf.setBoolean("mapred.intermediate.compress",true);
	    conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");   
	    conf.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);
	    
	    jobName = args[0];
	    
    	leftFileName = args[1];
    	leftFileDelimiter = args[2];
    	leftFileCached = Boolean.parseBoolean(args[3]);
    	
    	rightFileName = args[4];
    	rightFileDelimiter = args[5];
    	rightFileCached = Boolean.parseBoolean(args[6]);
    	
    	outputFileName = args[7];
    	outputFileDelimiter = args[8];
    	
    	joinType = args[9];
    	
    	conf.set("leftFileName", leftFileName);
    	conf.set("rightFileName", rightFileName);
    	conf.set("leftFileDelimiter", leftFileDelimiter);
    	conf.set("rightFileDelimiter", rightFileDelimiter);
    	conf.setBoolean("leftFileCached", leftFileCached);
    	conf.setBoolean("rightFileCaced", rightFileCached);
    	conf.set("outputFileDelimiter", outputFileDelimiter);
    	conf.set("joinType", joinType);
    	
	    System.out.println("jobName: " + jobName);
	    System.out.println("leftFileName: " + leftFileName);
	    System.out.println("leftFileDelimiter: " + leftFileDelimiter);
	    System.out.println("leftFileCached: " + leftFileCached);
	    System.out.println("rightFileName: " + rightFileName);
	    System.out.println("rightFileDelimiter: " + rightFileDelimiter);
	    System.out.println("rightFileCached: " + rightFileCached);
	    System.out.println("outputFile: " + outputFileName);
	    
    	
	    Job job = Job.getInstance(conf, jobName);
	    job.setJarByClass(MapSideJoinWithSort.class);
	    job.setMapperClass(MapSideJoinMapper.class);
	    job.setReducerClass(MapSideJoinReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    if(leftFileCached)
	    {
	    	System.out.println("left file cached: " + leftFileName);
	    	job.addCacheFile(new Path(leftFileName).toUri());
	    }
	    else
	    {
	    	System.out.println("left file is not cached: " + leftFileName);
	    	FileInputFormat.addInputPath(job, new Path(leftFileName));
	    }
	    
	    
	    if(rightFileCached)
	    {
	    	System.out.println("right file cached: " + leftFileName);
	    	job.addCacheFile(new Path(rightFileName).toUri());
	    }
	    else
	    {
	    	System.out.println("right file is not cached: " + rightFileName);
	    	FileInputFormat.addInputPath(job, new Path(rightFileName));
	    }
	    
	   
	    System.out.println("output file is: " + outputFileName);
	    File outputDirectory = new File(outputFileName);
	    
	    if(outputDirectory.exists())
	    {
	    	outputDirectory.delete();
	   	}
	    
	    FileOutputFormat.setOutputPath(job, new Path(outputFileName));
	 
	    job.waitForCompletion(true);

 }
}
