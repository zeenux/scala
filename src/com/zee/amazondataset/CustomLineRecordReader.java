/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.*;
/**
 *
 * @author zeenux
 */
public class CustomLineRecordReader extends RecordReader<LongWritable,Text> {

    private long start;
    private long pos;
    private long end;
    private CustomRecordReaderExtendingLineReader in;
    private int maxLineLength;
    private static int lineNo=0;
    private LongWritable key=new LongWritable();
    private Text value=new Text();
    
    private static final Log LOG=LogFactory.getLog(CustomLineRecordReader.class);
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        //System.out.println("Custom Line Record Reader Called");
        FileSplit split=(FileSplit) genericSplit;
        
        Configuration job=context.getConfiguration();
        this.maxLineLength=job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        // Split "S" is responsible for all records
        // starting from "start" and "end" positions
        start=split.getStart();
        end=start+split.getLength();
        System.out.println("start="+start+" || end="+end);
        final Path file=split.getPath();
        FileSystem fs=file.getFileSystem(job);
        FSDataInputStream fileIn=fs.open(split.getPath());
        
        boolean skipFirstLine=false;
        if(start!=0)
        {
            skipFirstLine=true;
            --start;
            fileIn.seek(start);
        }
        in=new CustomRecordReaderExtendingLineReader(fileIn,job);
        
        
        if(skipFirstLine) //==true
        {
            Text dummy=new Text();
            start+=in.readLine(dummy,0,(int)Math.min((long)Integer.MAX_VALUE, end-start));
            
        }
        
        this.pos=start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        key.set(pos);
        int newSize=0;
        lineNo++;
        System.out.println("Value is "+value.toString()+" And Line Number is "+lineNo);
        while(pos<end){
            newSize=in.readLine(value,maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), maxLineLength));
            System.out.println("NewSize is "+newSize);
            if(newSize==0) break;
            
            
            pos+=newSize;
            
            
            if(newSize<maxLineLength)  break;
            
            LOG.info("Skipped line of size "+newSize+" at pos "+(pos-newSize));
        }
            if(newSize==0){
                key=null;
                value=null;
                return false;
            }
            else{
                return true;
            }
        
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        System.out.println("Value called "+value);
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start==end) return 0.0f;
        else return Math.min(1.0f, (pos-start)/(float)end/start);
    }

    @Override
    public void close() throws IOException {
        if(in!=null) in.close();
    }
    
}
