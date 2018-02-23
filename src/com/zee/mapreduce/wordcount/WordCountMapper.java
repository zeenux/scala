/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.wordcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.Vector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author zeenux
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static int lineCount=0;
    Vector<WordsInMouth> vim=new Vector<WordsInMouth>();
    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws Exception
     */
    
    public class WordsInMouth{
        
        public String key, numInEnglish, numInUrdu;
        public WordsInMouth(String key, String nE, String nU){
            this.key=key;
            numInEnglish=nE;
            this.numInUrdu=nU;
           
            
        }
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
        
        //This Method is Called for Each Line.. Seperated by NewLine. 
       // System.out.println("Dumping Context");
       // System.out.println(context.toString());
      //  System.out.println("End Context Dumping");
        /*
        Here Text value is the COmplete Line. If we want to convert it into comma seperated then we have to find another way
        
        */
        
       // System.out.println("Key: "+key+" Value: "+value);
         String line = value.toString();
         
         lineCount++;
         //System.out.println("Number of Times Mapper called is "+lineCount);
         //System.out.println("The Complete line is "+line);
         String [] values=line.split(",");
         
         vim.add(new WordsInMouth(values[0],values[1],values[2]));
         
        for(String v:values){
            //System.out.println("\t"+v);
            word.set(v);
            
            context.write(word, one);
           // System.out.println("Context current Value is "+context.getCurrentValue()+" And the Key is "+context.getCurrentKey());
        }
        //System.out.println("THe Value is "+word.toString());
        // StringTokenizer tokenizer = new StringTokenizer(line);
        
        //while (tokenizer.hasMoreTokens()) {
          //  word.set(tokenizer.nextToken(","));
           // word.set(line.split(","));
        //context.write(new line.split(","), one);
        //System.out.println(word);
        
        
      }
    
    }
    

