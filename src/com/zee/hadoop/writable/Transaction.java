/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zee.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

/**
 *
 * @author zeenux
 */
public class Transaction implements Writable {
    private String Transaction_date,Product,Price,Payment_Type,	Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude;
    
    public static int transactionCount=0;
    public Transaction(String Transaction_date,String Product,String Price,String Payment_Type,String Name,String City,String State,String Country,String Account_Created,String Last_Login,String Latitude,String Longitude){
        transactionCount++;
        this.Transaction_date=Transaction_date;
        this.Product=Product;
        this.Price=Price;
        this.Payment_Type=Payment_Type;
        this.Name=Name;
        this.City=City;
        this.State=State;
        this.Country=Country;
        this.Account_Created=Account_Created;
        this.Last_Login=Last_Login;
        this.Latitude=Latitude;
        this.Longitude=Longitude;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(Name);
        out.writeBytes(Transaction_date);
        out.writeBytes(Product);
        out.writeBytes(Price);
        out.writeBytes(Payment_Type);
        out.writeBytes(this.City);
        out.writeBytes(this.State);
        out.writeBytes(this.Country);
        out.writeBytes(this.Account_Created);
        out.writeBytes(this.Last_Login);
        out.writeBytes(this.Latitude);
        out.writeBytes(this.Longitude);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Transaction_date=in.readLine();
        Product=in.readLine();
        Price=in.readLine();
        Payment_Type=in.readLine();
        Name=in.readLine();
        City=in.readLine();
        State=in.readLine();
        Country=in.readLine();
        Account_Created=in.readLine();
        Last_Login=in.readLine();
        Latitude=in.readLine();
        Longitude=in.readLine();
    }

    public String getTransaction_date() {
        return Transaction_date;
    }

    public void setTransaction_date(String Transaction_date) {
        this.Transaction_date = Transaction_date;
    }

    public String getProduct() {
        return Product;
    }

    public void setProduct(String Product) {
        this.Product = Product;
    }

    public String getPrice() {
        return Price;
    }

    public void setPrice(String Price) {
        this.Price = Price;
    }

    public String getPayment_Type() {
        return Payment_Type;
    }

    public void setPayment_Type(String Payment_Type) {
        this.Payment_Type = Payment_Type;
    }

    public String getName() {
        return Name;
    }

    public void setName(String Name) {
        this.Name = Name;
    }

    public String getCity() {
        return City;
    }

    public void setCity(String City) {
        this.City = City;
    }

    public String getState() {
        return State;
    }

    public void setState(String State) {
        this.State = State;
    }

    public String getCountry() {
        return Country;
    }

    public void setCountry(String Country) {
        this.Country = Country;
    }

    public String getAccount_Created() {
        return Account_Created;
    }

    public void setAccount_Created(String Account_Created) {
        this.Account_Created = Account_Created;
    }

    public String getLast_Login() {
        return Last_Login;
    }

    public void setLast_Login(String Last_Login) {
        this.Last_Login = Last_Login;
    }

    public String getLatitude() {
        return Latitude;
    }

    public void setLatitude(String Latitude) {
        this.Latitude = Latitude;
    }

    public String getLongitude() {
        return Longitude;
    }

    public void setLongitude(String Longitude) {
        this.Longitude = Longitude;
    }
    public String toString(){
        return Transaction_date+"|"+Product+"|"+Price+"|"+Payment_Type+"|"+Name+"|"+City+"|"+State+"|"+Country+"|"+Account_Created+"|"+Last_Login+"|"+Latitude+"|"+Longitude;
    }
}
