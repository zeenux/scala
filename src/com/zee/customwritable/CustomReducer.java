/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customwritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class CustomReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    
    @Override
    public void reduce(Text value,Iterable<IntWritable> itr,Context context) throws IOException, InterruptedException{
        int count=0;
        System.out.println(value);
        for(IntWritable i:itr){
        count+=i.get();
    }
        context.write(value, new IntWritable(count));
    }
}
