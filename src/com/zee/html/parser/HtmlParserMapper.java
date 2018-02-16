/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.html.parser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class HtmlParserMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
    
    private IntWritable one=new IntWritable(1);
    @Override
    public void map(LongWritable offset, Text record,Context context) throws IOException, InterruptedException{
        String [] lineSplits=record.toString().split(" ");
        
        for(String s: lineSplits){
            context.write(new Text(s), one);
        }
        
    }
}
