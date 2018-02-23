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
public class MapOnlyMapperNext extends Mapper<LongWritable, Text, Text, Text> {
    private final String outDelimiter="|";
    private double totalPrice;
    private String output;
        
    public void map(LongWritable offset, Text record, Context context) throws IOException, InterruptedException {
        // Get the Whole Line. Split it into Array Seperated by Comma. 
        String [] recordSplits=record.toString().split(","); 
        
        //Blah Blah
       // String [] customerSplits=recordSplits[7].toString().split("~");
       // String [] supplierSplits=recordSplits[8].split("~");
        try{
            // Calculate the Total Amount Due.. 
            totalPrice=Double.valueOf(recordSplits[2].trim())*Integer.valueOf(recordSplits[6]);
            output=totalPrice + "|"+recordSplits[6]+"|"+recordSplits[3];
        //System.out.println(totalPrice);
        }
        catch(NumberFormatException ee){
            //We will get this since there are headers and it can't calculate String * String.. 
            System.out.println("Caught a Bug.. "+ee.toString());
                    ee.printStackTrace();
        }
        
        
        
        //NOw Output this to the File Since there is No reducer. If there was a reducer this output would have
        // been outputed to Reducer.. Would be great if there is a Reducer but more on that Later on. 
        // This Part goes to the Reducer..
        try{
        context.write(new Text(recordSplits[4]), new Text(output));
        }
        catch(Exception eex){
            eex.printStackTrace();
        }
    }
}
