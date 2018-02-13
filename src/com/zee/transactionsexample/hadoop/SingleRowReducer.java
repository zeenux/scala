/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.transactionsexample.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;


/**
 *
 * @author zeenux
 */
public class SingleRowReducer extends Reducer<Text,Text,Text,Text>{
    private String output;
    @Override
    public void reduce(Text paymentType, Iterable<Text> records, Context context) throws IOException, InterruptedException {
        
        double sumPrice=0d;
        int minQuantity=Integer.MAX_VALUE;
        double maxUnitPrice=Double.MIN_VALUE;
        long count=0;
        for(Text r : records){
            String [] recordSplit=r.toString().split("\\|");
            sumPrice+=Double.valueOf(recordSplit[0]);
            minQuantity=Math.min(minQuantity, Integer.valueOf(recordSplit[1]));
            maxUnitPrice=Math.max(maxUnitPrice, Double.valueOf(recordSplit[2]));
            count+=Long.valueOf(recordSplit[2]);
            
        }
        
        output=sumPrice+"|"+minQuantity+"|"+maxUnitPrice+"|"+count;
        context.write(paymentType, new Text(output));
        
    }
    
}
