/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.titanic;
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
 
import org.apache.hadoop.mapreduce.InputSplit;
 
import org.apache.hadoop.mapreduce.RecordReader;
 
import org.apache.hadoop.mapreduce.TaskAttemptContext;
 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 *
 * @author zeenux
 */
public class Titanic_input extends FileInputFormat<Key_value,IntWritable> {

    @Override     //AbStract Method Required to Be Implemented
    public RecordReader<Key_value, IntWritable> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        return new MyRecordReader();
    }
    
    
}
