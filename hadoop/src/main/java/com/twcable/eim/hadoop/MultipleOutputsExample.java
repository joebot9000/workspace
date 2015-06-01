package com.twcable.eim.hadoop;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipleOutputsExample{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      while (itr.hasMoreTokens()) 
      {
        word.set(itr.nextToken());
        context.write(word, one);
      }
      
     
      String[] columns = value.toString().split(",");
      word.set(columns[2]);
      one.set(-1);
      context.write(word, one);
    }
  }
  
  
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
    	  if(val.get() == -1)
    	  {
    		  context.write(key, result);
    	  }
    	  else
    	  {
    		  sum += val.get();
    	  }

      
      }
      
      result.set(sum);
      context.write(key, result);
      
    }
  }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "multiple_output_example");
    job.setJarByClass(MultipleOutputsExample.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
    job.waitForCompletion(true);
    
    job = Job.getInstance(conf, "wordcount_output_example");
    job.setJarByClass(MultipleOutputsExample.class);
    job.setMapperClass(WordCountMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[1].replace("2", "3")));
    job.waitForCompletion(true);
    
    job = Job.getInstance(conf, "value_output_example");
    job.setJarByClass(MultipleOutputsExample.class);
    job.setMapperClass(ValueOutputMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[1].replace("2", "4")));
    job.waitForCompletion(true);
     
    
 }

  public static class WordCountMapper extends Mapper<Object, IntWritable, Text, IntWritable>{
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
      
       if(!value.toString().contains("-1"))
       {
        context.write(key, value);
       }
    }
  }
  
  public static class ValueOutputMapper extends Mapper<Object, IntWritable, Text, IntWritable>{
	    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
	      
	       if(value.toString().contains("-1"))
	       {
	    	   context.write(key, value);
	       }
	    }
	  }

  }

