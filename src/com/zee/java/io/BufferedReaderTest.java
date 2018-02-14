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
public class BufferedReaderTest {

    public static void ReadAFile(String path)throws IOException {
        BufferedReader br=new BufferedReader(new FileReader(path));
        String line=br.readLine();
        while(line!=null){
            System.out.println(line);
            line=br.readLine();
        }
    }
}
