/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.mapreduce.wordcount;

/**
 *
 * @author zeenux
 */
public class WordAndItsCount {
    static int objCount=0;
    private String word;
    private int count;
    
    public WordAndItsCount(){
        objCount++;
    }
    
    public String getWord(){
        return word;
    }
    
    public int getCount(){
        return count;
    }
    public void setWord(String word){
        this.word=word;
    }
    public void setCount(int count){
        this.count=count;
    }
    @Override
    public String toString(){
        return "Word "+word+" has a count of "+count;
    }
    
}
