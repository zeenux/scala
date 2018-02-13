/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.hadoop.bigram.example;

/**
 *
 * @author zeenux
 */
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class BigramCountReducer extends Reducer<TextPair, IntWritable, Text, IntWritable>{
    private static Text textPairText = new Text();
    
    
    public void reduce(Text key, Iterable values, Context context) throws IOException, InterruptedException
    {
        
        int count=0;
        /*for(IntWritable value: values)
        {
            count += value.get();
        }*/
 
        textPairText.set(key.toString());
        context.write(textPairText, new IntWritable(count));
    }
}
