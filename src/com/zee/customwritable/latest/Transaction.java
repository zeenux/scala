/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customwritable.latest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author zeenux
 */
public class Transaction implements Writable {
    private double sumPrice;
    private int minQuantity;
    private double maxUnitPrice;
    private long count;
    public Transaction(){
        this.sumPrice=0d;
        this.minQuantity=0;
        this.maxUnitPrice=0d;
        this.count=0l;
    }
    public Transaction (double sumPrice, int minQuantity, double maxUnitPrice, long count){
        this.sumPrice=sumPrice;
        this.minQuantity=minQuantity;
        this.maxUnitPrice=maxUnitPrice;
        this.count=count;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        setSumPrice(in.readDouble());
        setMinQuantity(in.readInt());
        setMaxUnitPrice(in.readDouble());
        setCount(in.readLong());
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(getSumPrice());
        out.writeInt(getMinQuantity());
        out.writeDouble(getMaxUnitPrice());
        out.writeLong(getCount());
    }

    public String toString(){
        return getSumPrice() +"|"+getMinQuantity()+"|"+getMaxUnitPrice()+"|"+getCount();
    }

    /**
     * @return the sumPrice
     */
    public double getSumPrice() {
        return sumPrice;
    }

    /**
     * @param sumPrice the sumPrice to set
     */
    public void setSumPrice(double sumPrice) {
        this.sumPrice = sumPrice;
    }

    /**
     * @return the minQuantity
     */
    public int getMinQuantity() {
        return minQuantity;
    }

    /**
     * @param minQuantity the minQuantity to set
     */
    public void setMinQuantity(int minQuantity) {
        this.minQuantity = minQuantity;
    }

    /**
     * @return the maxUnitPrice
     */
    public double getMaxUnitPrice() {
        return maxUnitPrice;
    }

    /**
     * @param maxUnitPrice the maxUnitPrice to set
     */
    public void setMaxUnitPrice(double maxUnitPrice) {
        this.maxUnitPrice = maxUnitPrice;
    }

    /**
     * @return the count
     */
    public long getCount() {
        return count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(long count) {
        this.count = count;
    }
}
