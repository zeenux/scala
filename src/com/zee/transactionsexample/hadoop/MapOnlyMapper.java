/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.transactionsexample.hadoop;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class MapOnlyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final String outDelimiter="|";
    private double totalPrice;
    
    public void map(LongWritable offset, Text record, Context context) throws IOException, InterruptedException {
        // Get the Whole Line. Split it into Array Seperated by Comma. 
        String [] recordSplits=record.toString().split(","); 
        //Blah Blah
        String [] customerSplits=recordSplits[7].toString().split("~");
        String [] supplierSplits=recordSplits[8].split("~");
        try{
            // Calculate the Total Amount Due.. 
            totalPrice=Double.valueOf(recordSplits[2].trim())*Integer.valueOf(recordSplits[6]);
            //System.out.println(totalPrice);
        }
        catch(NumberFormatException ee){
            //We will get this since there are headers and it can't calculate String * String.. 
            System.out.println("Caught a Bug.. "+ee.toString());
                    ee.printStackTrace();
        }
        
        // Now Lets Build a String.. by storing every field of the Recordset in a StringBuilder...
        StringBuilder sb=new StringBuilder(recordSplits[0]).append(outDelimiter);
        sb.append(recordSplits[1]).append(outDelimiter);
        sb.append(recordSplits[2]).append(outDelimiter);
        sb.append(totalPrice).append(outDelimiter);
        sb.append(recordSplits[4]).append(outDelimiter);
        sb.append(recordSplits[5]).append(outDelimiter);
        sb.append(recordSplits[0]).append(outDelimiter);
        //sb.append(recordSplits[1]+" "+customerSplits[2]).append(outDelimiter);
        sb.append(recordSplits[3]).append(outDelimiter);
  //      sb.append(supplierSplits[0]).append(outDelimiter);
//        sb.append(supplierSplits[2]);//.append(outDelimiter);

        //NOw Output this to the File Since there is No reducer. If there was a reducer this output would have
        // been outputed to Reducer.. Would be great if there is a Reducer but more on that Later on. 
        
        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
