/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.transactionsexample.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author zeenux
 */

public class Driver extends Configured implements Tool {

    public static void main(String [] args) throws Exception{
        int result=ToolRunner.run(new Configuration(), new Driver(),args);
        System.exit(result);
        
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        Job job=Job.getInstance(conf,"Map Job Only Demo");
        job.setJarByClass(Driver.class);
        
        //job.setMapperClass(MapOnlyMapper.class);
        
        //job.setMapperClass(SingleRowRetrievalMapper.class);
        job.setMapperClass(MapOnlyMapperNext.class);
        job.setReducerClass(SingleRowReducer.class);
        //job.setNumReduceTasks(0);
        //job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //job.setOutputKeyClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        FileInputFormat.addInputPath(job, new Path("/home/zeenux/Desktop/data2.csv"));
       // FileOutputFormat.setOutputPath(job, new Path(args[1]));
       double r=Math.random();
        FileOutputFormat.setOutputPath(job, new Path("/home/zeenux/op/result"+r));
        
        return job.waitForCompletion(true)?0:1;
        
    } 
}
