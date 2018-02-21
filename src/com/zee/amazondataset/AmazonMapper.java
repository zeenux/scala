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
public class AmazonMapper extends Mapper<LongWritable,Text,Text,AmazonRecord> {
    
    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
       // System.out.println("*******");
      //  System.out.println(value.toString());
      //  System.out.println("*******");
        String line=value.toString();//.split(" ");
        
        
        //myLine.stream().filter(c->c.contains("title")).forEach(System.out::println);
        
            if(line.contains("title")){
                String [] tmp=line.split(":");
                context.write(new Text("Book Title"), new AmazonRecord(1L,tmp[1]));
                System.out.println("Value of Tmp is "+tmp[1]);
            }
        
        //System.out.println("---KEY-----"+key+"--VALUE----"+value);
    }
    
}
