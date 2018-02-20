/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customwritable.latest;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class TransactionMapper extends Mapper<LongWritable,Text,Text,Transaction> {
private double totalPrice;
public void map(LongWritable offset, Text record, Context context) throws IOException, InterruptedException{
    String [] recordSplits=record.toString().split(",");
    totalPrice=Double.valueOf(recordSplits[3])*Integer.valueOf(recordSplits[5]);
    System.out.println("Product "+recordSplits[2]+" total Price is "+totalPrice+" Some other Number "+recordSplits[6]);
    context.write(new Text(recordSplits[4]), new Transaction(totalPrice,Integer.valueOf(recordSplits[6]),Double.valueOf(recordSplits[3]),1L));
}    
}
