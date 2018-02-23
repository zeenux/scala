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
public class MainClass {
    public static void main(String [] args){
        CreateFile cf=CreateFile.getInstance();
        CreateFile cf2=CreateFile.getInstance();
        try{
            cf2.CreateAFile("/home/zeenux/test--0.txt");
        
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
        try{
            ReadFile.readAFile("/home/zeenux/results.txt");
            BufferedReaderTest.ReadAFile("/home/zeenux/Student.txt");
        }
        catch(IOException ie){
            System.out.println(ie.getMessage());
        }
    }
}
