/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.mapreducecombiner;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 *
 * @author zeenux
 */
public class Driver extends Configured implements Tool {
    
    public static void main(String [] args) throws Exception{
        int result=ToolRunner.run(new Configuration(), new Driver(),args);
        System.exit(result);
    }
    
    public int run(String [] args) throws Exception{
        
        Configuration conf=getConf();
        Job job=Job.getInstance(conf, "Map Reduce with Combiner");
        job.setJarByClass(Driver.class);
        job.setMapperClass(TransactionMapper.class);
        job.setCombinerClass(TransactionCombiner.class);
        job.setReducerClass(TransactionReducer.class);
        job.setNumReduceTasks(2);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("/home/zeenux/DATASETS/Transactions.csv"));
        double rnd=Math.random();
        FileOutputFormat.setOutputPath(job, new Path("/home/zeenux/op/FILE_"+rnd));
        return job.waitForCompletion(true)?0:1;
    }
    
}
