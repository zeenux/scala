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
public class AmazonMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    
    LongWritable one=new LongWritable(1);
    private Text out = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
           System.out.println(value.toString());
        /*
        String line=value.toString();//.split(" ");
        
        String title="";
        String asin="";
       
        List<String> ar=Arrays.asList(line);
        
            if(line.contains("title")){
                String [] tmp=line.split(":");
                title=tmp[1];
               
            }
            if(line.contains("ASIN"))
            {
                String [] a=line.split(":");
                asin=a[1];
            }
        context.write(one, NullWritable.get());*/
        //out.set(key + " -------------\n" + value);
        //context.write(out, NullWritable.get());
    }
    
}
