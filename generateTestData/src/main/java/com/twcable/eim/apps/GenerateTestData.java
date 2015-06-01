package com.twcable.eim.apps;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class GenerateTestData{

        	private static int customerLineCount;
			private static int transactionLineCount;
			private static String customerFilePath;
			private static String transactionFilePath;
        	
        public static void main (String [] args) throws Exception{
        		customerLineCount = Integer.parseInt(args[1]);
        		customerFilePath = args[0];
        		transactionLineCount = Integer.parseInt(args[3]);
        		transactionFilePath = args[2];        	
        		
                generateCustomerFile();
                generateTransactionFile();
        }

		private static void generateCustomerFile() {
			try{
			        Path pt=new Path(customerFilePath);
			        FileSystem fs = FileSystem.get(new Configuration());
           
			        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			        
			        int lineCnt = customerLineCount;
			   		String line;
			        
			        for(int i=1; i<lineCnt; i++)
			        {
			        	//customer id, customer name, company
			        	String iString = Integer.toString(i);
			        	 line="00000000".substring(iString.length())+iString + ",John Doe,Time Warner Cable, 2014\n";
			        	 //System.out.println(line);
			        	 br.write(line);
			        }


			        br.close();
			}catch(Exception e){
			        System.out.println("File not found");
			        System.out.println(e.getMessage());
			}
		}
		
		private static void generateTransactionFile() {
			try{
					Configuration conf = new Configuration();
					conf.setInt("dfs.block.size", 256000000);
			        Path pt=new Path(transactionFilePath);
			        FileSystem fs = FileSystem.get(conf);
           
			        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			        
			        int lineCnt = transactionLineCount;
			        //int lineCnt = 10;
			        Random customerId = new Random();
			        Random tranId = new Random();
			        String line;
			        
			        for(int i=1; i<lineCnt; i++)
			        {
			        	String sCustomerId = Integer.toString(customerId.nextInt((customerLineCount - 0) + 1) + 0);
			        	String sTransactionId = Integer.toString(tranId.nextInt((8000 - 7000) + 1) + 7000);
			        	line="00000000".substring(sCustomerId.length())+sCustomerId + ",Phone Call,Mary Jane,Details about the call. Details about the call. Details about the call. Details about the call.," + sTransactionId + "\n";
			        	br.write(line);
			        }

			        br.close();
			}catch(Exception e){
			        System.out.println("File not found");
			        System.out.println(e.getMessage());
			}
		}
        

}