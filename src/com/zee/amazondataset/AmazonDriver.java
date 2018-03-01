/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author zeenux
 */
public class AmazonDriver extends Configured implements Tool {
    
    public static void main(String [] args) throws Exception{
        
        int result=ToolRunner.run(new Configuration(), new AmazonDriver(), args);
    }
    
    public int run(String [] args) throws Exception{
        Configuration cnf=getConf();
        String regex = "^\n";

        cnf.set("record.delimiter.regex", regex);
        
        Job job=Job.getInstance(cnf,"Amazon DataSet");
        job.setJarByClass(AmazonDriver.class);
        job.setMapperClass(AmazonMapper.class);
       // job.setCombinerClass(AmazonReducer.class);
       // job.setReducerClass(AmazonReducer.class);
        //<LongWritable,Text,LongWritable,AmazonRecord>
        job.setMapOutputKeyClass(LongWritable.class);
        job.setInputFormatClass(PatternInputFormat.class);
    //job.setMapOutputValueClass(BestRecordReader.class);
        job.setNumReduceTasks(0);
       // job.setOutputKeyClass(Text.class);
       // job.setOutputValueClass(BestRecordReader.class);
        //job.setInputFormatClass(PatternInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        //job.setMapOutputKeyClass(theClass);
        //job.setNumReduceTasks(2);
        
        FileInputFormat.addInputPath(job, new Path("/home/zeenux/DATASETS/amazon-meta-subset.txt"));
        double rnd=Math.random();
        FileOutputFormat.setOutputPath(job, new Path("/home/zeenux/op/op_"+rnd));
        
        return job.waitForCompletion(true)?0:1;
    }
    
}
