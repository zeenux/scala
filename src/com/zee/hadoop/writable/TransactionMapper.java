/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.hadoop.writable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class TransactionMapper extends Mapper<LongWritable,Text,Text,Transaction>  {
    @Override
    public void map(LongWritable offset, Text record, Context context)throws IOException, InterruptedException{
        String [] recordSplits=record.toString().split(",");
        context.write(new Text(recordSplits[1]),new Transaction(recordSplits[0],recordSplits[1],recordSplits[2],recordSplits[3],recordSplits[4],recordSplits[5],recordSplits[6],recordSplits[7],recordSplits[8],recordSplits[9],recordSplits[10],recordSplits[11]));
    }
        
}
