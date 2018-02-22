/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.titanic;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class TitanicReduce extends Reducer<Key_value,IntWritable,Key_value,IntWritable>  {
    
    @Override
    public void reduce(Key_value key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum=0;
        for(IntWritable val:values){
            sum+=val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
