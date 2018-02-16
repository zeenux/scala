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
public class HtmlParserReducer extends Reducer<Text,IntWritable,IntWritable,Text> {
    
    public int count;
    public void reduce(Text value, Iterable<Text> records, Context context) throws IOException, InterruptedException{
        
        for(Text r:records){
            count++;
            context.write(new IntWritable(count), r);
        }
        
    }
}
