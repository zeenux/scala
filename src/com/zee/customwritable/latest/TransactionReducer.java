/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.customwritable.latest;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class TransactionReducer extends Reducer<Text,Transaction,Text,Transaction> {
    private String output;
    @Override
    public void reduce(Text paymentType, Iterable<Transaction> transactions, Context context) throws IOException, InterruptedException{
        double sumPrice=0d;
        int minQuantity=Integer.MAX_VALUE;
        double maxUnitPrice=Double.MIN_VALUE;
        long count=0;
        for(Transaction record: transactions){
            sumPrice+=record.getSumPrice();
            minQuantity=Math.min(minQuantity, record.getMinQuantity());
            maxUnitPrice=Math.max(maxUnitPrice, record.getMaxUnitPrice());
            count+=Long.valueOf(record.getCount());
        } 
        System.out.println();
        context.write(paymentType, new Transaction(sumPrice,minQuantity,maxUnitPrice,count));
    }
}
