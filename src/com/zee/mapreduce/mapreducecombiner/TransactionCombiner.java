/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.mapreducecombiner;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


/**
 *
 * @author zeenux
 */
public class TransactionCombiner extends Reducer<Text,Text,Text,Text> {
    private String output;
    
    @Override
    public void reduce(Text paymentType, Iterable<Text> records, Context context) throws IOException, InterruptedException{
        double sumPrice=0d;
        int minQuantity=Integer.MAX_VALUE;
        double unitPrice=Double.MIN_VALUE;
        double maxUnitPrice=Double.MIN_VALUE;
        long count=0;
        for(Text record:records){
            String [] recordSplit=records.toString().split("\\|");
            sumPrice+=Double.valueOf(recordSplit[0]);
            minQuantity=Math.min(minQuantity, Integer.valueOf(recordSplit[1]));
            maxUnitPrice=Math.min(maxUnitPrice, Double.valueOf(recordSplit[2]));
            count+=Long.valueOf(recordSplit[3]);
        }
        
        output=sumPrice+"|"+minQuantity+"|"+maxUnitPrice+"|"+count;
        context.write(paymentType,new Text(output));
    }
}
