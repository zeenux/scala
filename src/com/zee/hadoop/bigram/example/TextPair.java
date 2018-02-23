/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.hadoop.bigram.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;
/**
 *
 * @author zeenux
 */
public class TextPair implements WritableComparable {

    private Text first;
    private Text second;
    
    public TextPair(Text first, Text second){
        set(first,second);
    }
    
    public TextPair(){
        set(new Text(),new Text());
    }
    public TextPair(String first, String second){
        set(new Text(first),new Text(second));
    }
    public Text getFirst(){
            return first;
    }
    public Text getSecond(){
        return second;
    }
    
    public void set(Text first, Text second){
        this.first=first;
        this.second=second;
    }
    @Override
    public void write(DataOutput d) throws IOException {
        first.write(d);
        second.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
    first.readFields(di);
    second.readFields(di);
    
    }

    @Override
    public int compareTo(Object o) 
    {
        TextPair tp=(TextPair)o;
        int cmp=first.compareTo(tp.first);
        if(cmp!=0)
            return cmp;
        
        return second.compareTo(tp.second);
    }
    @Override
    public String toString(){
        return first + " "+ second;
    }
    
    @Override
    public int hashCode(){
        return first.hashCode()*163+second.hashCode();
    }
    @Override
    public boolean equals(Object o){
        if(o instanceof TextPair){
        TextPair tp=(TextPair)o;
        return first.equals(tp.first)&&second.equals(tp.second);
    }
        return false;
    }
}
