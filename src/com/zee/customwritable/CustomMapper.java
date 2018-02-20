/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customwritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;
/**
 *
 * @author zeenux
 */                                   //input1  input2 output1 output2
public class CustomMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    
    @Override            //Key   (1,"HELLO DOODLE") //Value
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        System.out.println(key);
        //This method gets a whole line right up to the New Line in the Form of a Record.. 
        String line=value.toString();
        
        System.out.println(line);
        for(String word:line.split(",")){
            if(word.trim().length()>0)
                //FOr Every Word Make a Key/Value Pair For E.g ("Hello Doodle",1). This will output the
                // Key/Value Pair to the Reducer if a Reducer Exists and is assigned in the Driver Class...
                //Hence this output would be the Input for Reduce class. 
                context.write(new Text(word), new IntWritable(1));
        }
        
    }
}
