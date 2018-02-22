/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.titanic;
import java.io.DataInput;
 
import java.io.DataOutput;
 
import java.io.IOException;
 
import org.apache.hadoop.io.*;
 
import com.google.common.collect.ComparisonChain;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
/**
 *
 * @author zeenux
 */
public class Key_value implements WritableComparable<Key_value> {

    private String x;
    private String y;
    public String getX(){
        return x;
    }
    public String getY(){
        return y;
    }
    public void setX(String x){
        this.x=x;
    }
    
    public void setY(String y){
        this.y=y;
    }
    
    public Key_value(String x, String y){
        this.x=x;
        this.y=y;
    }
    
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(x);
        out.writeUTF(y);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x=in.readUTF();
        y=in.readUTF();
    }
    
    public Key_value(){
        // Required for First Initalization
    }

    @Override
    public int compareTo(Key_value o) {
        /*
        In the compareTo method, we have written our logic to sort the keys by the gender column. 
        We have taken the ComparisionChain class and first compared the gender column 
        and then compared the 1st column. Therefore, this logic will print the keys sorted by Gender column.
        --------- NOTE ------
        If you compare only one column, then the second 
        will be considered as a single value by the WritableComparable interface.
        -------- END NOTE ------
        */
       return ComparisonChain.start().compare(this.y,o.y).compare(this.x, o.x).result();
    }
    
    public boolean equals(Object o1){
        if (!(o1 instanceof Key_value)) {
            return false;
        }
        Key_value other=(Key_value)o1;
        return this.x==other.x && this.y==other.y;
    }
    
    @Override
    public String toString(){
        return x.toString()+","+y.toString();
    }
}
