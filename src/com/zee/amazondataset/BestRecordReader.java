/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;

import java.util.regex.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.*;
import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
/**
 *
 * @author zeenux
 */
public class BestRecordReader extends RecordReader<LongWritable,Text>{
    private LineReader in;
    private final static Text EOL=new Text("\n");
    private Pattern delimiterPattern;
    private String delimiterRegex;
    private int maxLengthRecord;
    private LongWritable key=new LongWritable();
    private Text value=new Text();
    private int pos;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException{
        System.out.println("Called Best Record Reader");
        Configuration job=context.getConfiguration();
        this.delimiterRegex=job.get("record.delimiter.regex");
        this.maxLengthRecord=job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        delimiterPattern=Pattern.compile(delimiterRegex);
    }
    
    
    private int readNext(Text text, int maxLineLength, int maxBytesToConsume)throws IOException {
        int offset=0;
        text.clear();
        Text tmp=new Text();
        for(int i=0;i<maxBytesToConsume;i++){
            int offsetTmp=in.readLine(tmp, maxLineLength, maxBytesToConsume);
            offset+=offsetTmp;
            Matcher m=delimiterPattern.matcher(tmp.toString());
            if(offsetTmp==0){
                break;
            }
            if(m.matches()){
                break;
            }
            else{
                text.append(EOL.getBytes(), 0, EOL.getLength());
                text.append(tmp.getBytes(), 0, tmp.getLength());
            }
        }
        System.out.println(text.toString());
        value=text;
        key=new LongWritable(offset);
        
        return offset;
    
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
         return 0.0f;
        
    }

    @Override
    public void close() throws IOException {
        if(in!=null)
            in.close();
    }
    
    
    
}
