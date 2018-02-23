/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.wordcount;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;
/**
 *
 * @author zeenux
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    public static int sadSum=0;
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        //System.out.println("Context Current Value = "+context.getCurrentValue());
        //System.out.println("Context Current Key = "+context.getCurrentKey());
        int sum=0;
        String [] choiceWords=new String[10];
        Vector<String> v1=new Vector<String>();
        v1.add("sad");
        v1.add("night");
        v1.add("one");
        Vector<WordAndItsCount> v2=new Vector<WordAndItsCount>();
        for(IntWritable value: values){
          //  System.out.println(key);
            for(String s:v1){
               // if(key.toString().toLowerCase().contains("one"))
               // {
                    sadSum++;
                    WordAndItsCount w=new WordAndItsCount();
                    w.setWord(key.toString());
                    w.setCount(sum);
                    v2.add(w);
                    System.out.println(sadSum+key.toString());
                    sum++;
                    context.write(key, new IntWritable(sum));
                //}
            }
            
            System.out.println("The Size is "+WordAndItsCount.objCount);
            
            //sum+=value.get();
            
        }
        
    }
}
