/*
 *  Author: Manu Mukerji <next2manu@gmail.com>
 * 
 */

package com.example.com.mr_cassandra;

import com.example.com.mr_cassandra.CassandraHelper;

public class CassandraTester {
    
    public static void main(String[] args) throws InterruptedException {
         
        String host = "";
        
        CassandraHelper client = new CassandraHelper();
        
        //Create the connection
        client.createConnection(host);
        
        System.out.println("starting writes");
        
        //Add test value
        client.addKey("test1234");
        
        //Close the connection
        client.closeConnection();
        
        System.out.println("Write Complete");
    }
}