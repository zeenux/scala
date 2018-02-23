/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.java.io;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class CreateFile {
    //A SingleTon Class 
    
    public static CreateFile instance =null;
    protected CreateFile(){
        
    }
    
    public static CreateFile getInstance(){
        if(instance==null)
            instance= new CreateFile();
            return instance;
    }
    
    public boolean CreateAFile(String fileName) throws IOException{
        File f=new File(fileName);
        return f.createNewFile();
    }
    
    
}
