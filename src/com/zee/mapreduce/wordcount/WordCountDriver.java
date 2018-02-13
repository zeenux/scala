/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author zeenux
 */
public class WordCountDriver extends Configured implements Tool {
    
    public static void main(String [] args) throws Exception{
        
        int res=ToolRunner.run(new WordCountDriver(), args);
        System.exit(res);
    }
    
    public int run(String [] args)  throws Exception{
        Path inputPath=new Path("/home/zeenux/data.txt");
        double rnd=Math.random();
        Path outputPath=new Path("/home/zeenux/op/ouput"+rnd);
        Configuration config=new Configuration();
        
        Job job= new Job(config,this.getClass().toString());
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        job.setJobName("WordCountProgram");
        job.setJarByClass(WordCountDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        
        return job.waitForCompletion(true) ?0:1;
        
        
        
    }
    
}
