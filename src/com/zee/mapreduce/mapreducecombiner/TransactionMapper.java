/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.mapreducecombiner;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class TransactionMapper extends Mapper<LongWritable, Text,Text,Text> {
    private double totalPrice;
    private String output;
    
    public void map(LongWritable offset, Text record,Context context) throws IOException, InterruptedException{
        String [] recordSplits=record.toString().split(",");
        totalPrice=Double.valueOf(recordSplits[3]);
        output=totalPrice+"|"+recordSplits[6]+"|"+recordSplits[3]+"|1";
        context.write(new Text(recordSplits[4]), new Text(output));
    }
}
