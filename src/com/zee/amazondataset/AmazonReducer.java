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
public class AmazonReducer extends Reducer<Text,AmazonRecord,Text,AmazonRecord> {
    
    String title="";
    int count=0;
    @Override
    public void reduce(Text tit, Iterable<AmazonRecord> records, Context context) throws IOException, InterruptedException{
        
        for(AmazonRecord ar:records){
             System.out.println("Getting Title in Reducer "+ar.getTitle());
            if(ar.getTitle().length()!=7)
            {
                title=ar.getTitle();
                count++;
                
            }
            
            
        }
        context.write(new Text(""), new AmazonRecord(Long.MIN_VALUE,title));
    }
}
