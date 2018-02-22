/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.LineReader;
/**
 *
 * @author zeenux
 */
public class PatternRecordReader extends RecordReader<LongWritable, Text> {
 
    private LineReader in;
    private final static Text EOL = new Text("\n");
    private Pattern delimiterPattern;
    private String delimiterRegex;
    private int maxLengthRecord;
 
    @Override
    public void initialize(InputSplit split,
                        TaskAttemptContext context)
            throws IOException, InterruptedException {
 
        Configuration job = context.getConfiguration();
        this.delimiterRegex = job.get("record.delimiter.regex");
        this.maxLengthRecord = job.getInt(
                                "mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
 
        delimiterPattern = Pattern.compile(delimiterRegex);
     
    }
 
    private int readNext(Text text,
                        int maxLineLength,
                        int maxBytesToConsume)
            throws IOException {
 
        int offset = 0;
        text.clear();
        Text tmp = new Text();
 
        for (int i = 0; i < maxBytesToConsume; i++) {
 
            int offsetTmp = in.readLine(
                                     tmp,
                                     maxLineLength,
                                     maxBytesToConsume);
            offset += offsetTmp;
            Matcher m = delimiterPattern.matcher(tmp.toString());
 
            // End of File
            if (offsetTmp == 0) {
                break;
            }
 
            if (m.matches()) {
                // Record delimiter
                break;
            } else {
                // Append value to record
                text.append(EOL.getBytes(), 0, EOL.getLength());
                text.append(tmp.getBytes(), 0, tmp.getLength());
            }
        }
        return offset;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
