/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.amazondataset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.*;
/**
 *
 * @author zeenux
 */
public class AmazonRecord implements Writable {
    public static int bookCount=0;
    private Text asin;
    private Text title;

    public AmazonRecord(String t, String asin){
        bookCount++;
        setTitle(t);
        setAsin(asin);
        
    }
    public void setAsin(String a){
        this.asin=new Text(a);
    }
    public String getAsin(){
        return asin.toString();
    }
    public AmazonRecord(){
       // bookCount++;
        this.title=new Text("");
        this.asin=new Text("");
    }
    @Override
    public void write(DataOutput out) throws IOException {
       title.write(out);
       asin.write(out);
        
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        title.readFields(in);
        asin.readFields(in);
    }

    public String getTitle() {
        
        return title.toString();
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String t) {
        this.title=new Text(t);
    }
    @Override
    public String toString(){
        
        return title.toString()+asin.toString();
    }
}
