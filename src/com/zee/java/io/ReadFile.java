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
public class ReadFile {
    
    public static void readAFile(String path) throws IOException{
        File f=new File(path);
        BufferedInputStream bis=null;
        FileInputStream fis=new FileInputStream(f);
        bis=new BufferedInputStream(fis);
        while(bis.available()>0){
            System.out.print((char)bis.read());
        }
        
    }
    
}
