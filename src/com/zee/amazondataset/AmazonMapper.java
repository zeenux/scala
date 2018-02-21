/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import java.util.Arrays;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class AmazonMapper extends Mapper<LongWritable,Text,LongWritable,AmazonRecord> {
    
    LongWritable one=new LongWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
       
        String line=value.toString();//.split(" ");
        
        String title="";
        String asin="";
       
        
            if(line.contains("title")){
                String [] tmp=line.split(":");
                title=tmp[1];
               
            }
            else if(line.contains("ASIN"))
            {
                String [] a=line.split(":");
                asin=a[1];
            }
        context.write(one, new AmazonRecord(title,asin));
       
    }
    
}
