/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.wordcount;

import java.io.*;
import org.apache.hadoop.io.*;

/**
 *
 * @author zeenux
 */
public class MyWritable implements WritableComparable<MyWritable> {
    /*\Hadoop makes an heavy use of network transmissions for executing its jobs
    Every object you need to emit from mapper to reducers or as an output 
    has to implement this Writable interface in order to make Hadoop trasmit the data from/to the nodes in the cluster. 
    */
    private int counter;
       private long timestamp;
       
       public void write(DataOutput out) throws IOException {
           //method, write() is used for writing the data onto the stream,
         out.writeInt(counter);
         out.writeLong(timestamp);
       }
       
       public void readFields(DataInput in) throws IOException {
           //readFields(), is used for reading data from the stream.
         counter = in.readInt();
         timestamp = in.readLong();
       }
       
       public static MyWritable read(DataInput in) throws IOException {
         MyWritable w = new MyWritable();
         w.readFields(in);
         return w;
       }

    @Override
    public int compareTo(MyWritable o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
