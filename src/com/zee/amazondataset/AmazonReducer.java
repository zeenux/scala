/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class AmazonReducer extends Reducer<LongWritable,AmazonRecord,LongWritable,AmazonRecord> {
    
    
    @Override
    public void reduce(LongWritable tit, Iterable<AmazonRecord> records, Context context) throws IOException, InterruptedException{
      String title="";
      String asin="";
        int count=0;
      
        for(AmazonRecord ar:records){
             
            
                title=ar.getTitle();
                asin=ar.getAsin();
                count++;
                
            context.write(new LongWritable(1), new AmazonRecord(title,asin));
            
            
        }
        //System.out.println(count+" "+ new AmazonRecord(title).toString());
        
        
    }
}
