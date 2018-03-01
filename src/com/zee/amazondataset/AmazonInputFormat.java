/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
/**
 *
 * @author zeenux
 */
public class AmazonInputFormat extends FileInputFormat<Text, Text> {
    String data=new String();
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new AmazonInputFormatClass();
    }
    public class AmazonInputFormatClass extends RecordReader<Text, Text> {
        //private FileRecordReader fileRecordReader=null;
        private LineRecordReader lineRecordReader = null;
        private Text key = null;
        private Text value = null;

        @Override
        public void close() throws IOException {
            if (null != lineRecordReader) {
                lineRecordReader.close();
                lineRecordReader = null;
            }
            key = null;
            value = null;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            close();

            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!lineRecordReader.nextKeyValue()) {
                key = null;
                value = null;
                return false;
            }

            // This is where you should implement your custom logic.
            Text line = lineRecordReader.getCurrentValue();
            
            String str = line.toString();
            System.out.println(str);
            String[] arr = str.split("\\t");
            try{
                key = new Text(arr[1]);
                System.out.println(key);
                value = new Text(arr[4]);
                System.out.println(value);
            }
            catch(Exception ex){
                System.out.println(ex.toString());
            }

            return true;
        }

    }
    
}
