/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.mrcassandra;

/**
 *
 * @author myhome
 */
public class CassandraTester {

    public static void main(String[] args) throws InterruptedException {

        String host = "192.168.99.100";

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
