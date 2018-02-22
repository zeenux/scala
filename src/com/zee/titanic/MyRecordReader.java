/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.titanic;
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
 
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.InputSplit;
 
import org.apache.hadoop.mapreduce.RecordReader;
 
import org.apache.hadoop.mapreduce.TaskAttemptContext;
 
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
/**
 *
 * @author zeenux
 */
public class MyRecordReader extends RecordReader<Key_value,IntWritable>  {
    
    private Key_value key;
    private IntWritable value;
    private LineRecordReader reader=new LineRecordReader();

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        reader.initialize(is, tac);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        /*
        In the RecordReader, the nextKeyVlaue() is the method passed to our inputs. 
        From the dataset, this RecordReader will take each line as input and sets the columns into our custom key as follows:
        
        */
     boolean gotNextKeyValue=reader.nextKeyValue();
     if(gotNextKeyValue){
         if(key==null){
             key=new Key_value();
         }
         if(value==null){
             value=new IntWritable();
         }
         Text line=reader.getCurrentValue();
         String [] tokens=line.toString().split(",");
         /*
         As discussed earlier, we need 2nd and 5th columns passed to our custom key.
         The value is set as ‘1‘ since we need to count the number of people.
         */
         key.setX(new String(tokens[1]));
         key.setY(new String(tokens[4]));
         
         value.set(new Integer(1));
         
         
     }
     else
     {
         key=null;
         value=null;
     }
     return gotNextKeyValue;
    }

    @Override
    public Key_value getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
    
}
