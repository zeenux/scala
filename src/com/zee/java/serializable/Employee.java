/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.java.serializable;
import java.io.*;
/**
 *
 * @author zeenux
 */
public class Employee implements Serializable {
    public String name;
    public String address;
    public transient int SSN;
    public int number;
    
    public void mailCheck(){
        System.out.println("Mailing a check to "+name+" at Address "+address);
    }
    
}
