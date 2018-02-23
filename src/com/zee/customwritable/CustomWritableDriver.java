/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customwritable;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

/**
 *
 * @author zeenux
 */
public class CustomWritableDriver extends Configured implements Tool {
    
    public static void main(String [] args) throws Exception{
         int result=ToolRunner.run(new Configuration(), new CustomWritableDriver(),args);
        System.exit(result);
    }
    @Override
    public int run(String [] args) throws Exception{
        Configuration conf=new Configuration();
        //Since we want our record seperated by a Period . Hence we change the InputSplit
        
//conf.set("textinputformat.record.delimiter", ".");
        //Set Delimiter to Pipe |
        //conf.set("textinputformat.record.delimiter", ",");
        Job job=Job.getInstance(conf,"Custom Writable");  //Singleton
        
        job.setMapperClass(CustomMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(CustomReducer.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path("/home/zeenux/DATASETS/data_new.txt"));
        double rnd=Math.random();
        FileOutputFormat.setOutputPath(job, new Path("/home/zeenux/op/output_"+rnd));
        return job.waitForCompletion(true)?0:1;
    }
}
