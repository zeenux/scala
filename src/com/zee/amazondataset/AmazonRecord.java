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

/**
 *
 * @author zeenux
 */
public class AmazonRecord implements Writable {
    public static int bookCount=0;
    private Long id;
    private String title;

    public AmazonRecord(Long id, String title){
        bookCount++;
        this.id=Long.valueOf(bookCount);
        this.title=title;
        
        
    }
    public AmazonRecord(){
        this.id=0L;
        this.title=null;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.getId());
        out.writeChars(this.title);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.title=in.readLine();
        this.id=Long.valueOf(+bookCount);
    }

    /**
     * @return the id
     */
    public Long getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }
    public String toString(){
        return this.getId()+":"+this.getTitle();
    }
}
