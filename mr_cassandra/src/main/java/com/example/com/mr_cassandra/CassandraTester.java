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
        
        client.createConnection(host);
        
        System.out.println("starting writes");
        
        client.addKey("test1234");
        
        System.out.println("Write Complete");
    }
}

