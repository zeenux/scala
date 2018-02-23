/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.hadoop.writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
/**
 *
 * @author zeenux
 */
public class TransactionReducer extends Reducer<Text,Transaction, Text,Transaction> {
    long count=0;
    @Override
    public void reduce(Text paymentType, Iterable<Transaction> transactions, Context context){
        for(Transaction r:transactions){
            
        }
    }
    
}
