/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.titanic;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 *
 * @author zeenux
 */
public class TitanicDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
 
Configuration conf=new Configuration();
 
Job job=Job.getInstance(conf,"Find People In Titanci");// new Job();
 
job.setJarByClass(TitanicDriver.class);
 
job.setMapperClass(Map.class);
 
job.setReducerClass(TitanicReduce.class);
 
job.setOutputKeyClass(Key_value.class);
 
job.setMapOutputKeyClass(Key_value.class);
 
job.setMapOutputValueClass(IntWritable.class);
 
job.setOutputValueClass(IntWritable.class);
 
job.setInputFormatClass(Titanic_input.class);
 
job.setOutputFormatClass(TextOutputFormat.class);
 
//Path out=new Path("/home/zeenux/DATASETS/TitanicData.csv");
 
//out.getFileSystem(conf).delete(out);
 
FileInputFormat.addInputPath(job,new Path( "/home/zeenux/DATASETS/TitanicData.csv"));
 double rnd=Math.random();
FileOutputFormat.setOutputPath(job, new Path("/home/zeenux/op/output_"+rnd));
 
job.waitForCompletion(true);
 
}
}
