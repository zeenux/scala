/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 *
 * @author zeenux
 */
public class CustomFileInputFormat extends FileInputFormat<LongWritable,Text>{
 
    @Override
    public RecordReader<LongWritable, Text> createRecordReader( InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        
        return new CustomLineRecordReader();
    }
}
