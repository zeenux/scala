/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.transactionsexample.hadoop;
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
/**
 *
 * @author zeenux
 */
public class SingleRowRetrievalMapper extends Mapper<LongWritable, Text,NullWritable, Text> {
    
    public void map(LongWritable offset, Text record, Context context)throws IOException, InterruptedException{
        String [] records=record.toString().split(",");
        context.write(NullWritable.get(), new Text(records[5]));
    }
}
