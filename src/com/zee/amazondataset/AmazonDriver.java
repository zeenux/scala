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
        Job job=Job.getInstance(cnf,"Amazon DataSet");
        job.setJarByClass(AmazonDriver.class);
        job.setMapperClass(AmazonMapper.class);
        //job.setMapOutputKeyClass(theClass);
        job.setNumReduceTasks(0);
        
        FileInputFormat.addInputPath(job, new Path("/home/zeenux/DATASETS/amazon-meta-subset.txt"));
        double rnd=Math.random();
        FileOutputFormat.setOutputPath(job, new Path("/home/zeenux/op/op_"+rnd));
        
        return job.waitForCompletion(true)?0:1;
    }
    
}
