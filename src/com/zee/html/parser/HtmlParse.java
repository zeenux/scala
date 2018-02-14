/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.html.parser;

import java.io.IOException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import java.io.*;
import java.util.*;
/**
 *
 * @author zeenux
 */
public class HtmlParse {
    public static void main(String [] args) throws IOException{
        Document doc = Jsoup.connect("https://www.indy100.com/article/british-swear-words-ranked-ofcom-7340446").get();
        String data=doc.html();
        String txt=Jsoup.parse(data).text();
        File f=new File("/home/zeenux/Htmldata.txt");
        String [] q=txt.split(" ");
        List<String> myData=Arrays.asList(q);
        
        FileWriter fw=new FileWriter(f);
        fw.write(txt);
    }
    
}
