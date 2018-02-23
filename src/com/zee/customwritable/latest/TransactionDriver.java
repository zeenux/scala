/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customwritable.latest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author zeenux
 */ 
public class TransactionDriver extends Configured implements Tool {
    
    public static void main(String [] args) throws Exception{
        int result=ToolRunner.run(new Configuration(), new TransactionDriver(),args);
    }
    
    public int run(String [] args) throws Exception{
        Configuration conf=getConf();
        Job job=Job.getInstance(conf,"SSSS");
        job.setJarByClass(TransactionDriver.class);
        job.setMapperClass(TransactionMapper.class);
        job.setCombinerClass(TransactionReducer.class);
        job.setReducerClass(TransactionReducer.class);
        job.setNumReduceTasks(2);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Transaction.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Transaction.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("/home/zeenux/DATASETS/SalesJan2009.csv"));
        double rnd=Math.random();
        FileOutputFormat.setOutputPath(job, new Path("/home/zeenux/op/FILE_"+rnd));
        return job.waitForCompletion(true)?0:1;
        
    }
}
