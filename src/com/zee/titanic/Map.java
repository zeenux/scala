/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.titanic;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class Map extends Mapper<Key_value, Text, Key_value, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    @Override
    public void map(Key_value key, Text value, Context context ) throws InterruptedException, IOException{
        context.write(key, one);
    }
    
}
