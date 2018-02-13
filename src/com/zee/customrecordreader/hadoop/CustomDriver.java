/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customrecordreader.hadoop;

import com.zee.mapreduce.wordcount.WordCountDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author zeenux
 */
public class CustomDriver extends Configured implements Tool {
    
    public static void main(String [] args) throws Exception{
        int res=ToolRunner.run(new CustomDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Path inputPath=new Path("/home/zeenux/data.txt");
        double rnd=Math.random();
        Path outputPath=new Path("/home/zeenux/op/ouput"+rnd);
        Configuration config=new Configuration();
        
        Job job= new Job(config,this.getClass().toString());
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        job.setJobName("WordCountProgram");
        job.setJarByClass(WordCountDriver.class);
        job.setInputFormatClass(CustomFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);
        
        return job.waitForCompletion(true) ?0:1;
        
        
    }
    
}
