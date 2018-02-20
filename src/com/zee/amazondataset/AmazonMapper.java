/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
/**
 *
 * @author zeenux
 */
public class AmazonMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    
    @Override
    public void map(LongWritable key, Text value, Context context){
        System.out.println("*******");
        System.out.println(value.toString());
        System.out.println("*******");
        String [] line=value.toString().split("\n");
        
        
        for(String k:line){
            if(k.contains("title"))
            System.out.println(k);
            else if(k.contains("rating"))
                System.out.println(k);
        }
        //System.out.println("---KEY-----"+key+"--VALUE----"+value);
    }
    
}
