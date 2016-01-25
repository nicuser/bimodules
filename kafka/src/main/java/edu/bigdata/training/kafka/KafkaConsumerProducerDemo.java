/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.kafka;

/**
 *
 * @author myhome
 */
public class KafkaConsumerProducerDemo {

    String ZK_CONNECT = "127.0.0.1:2181";
    String GROUP_ID = "group1";
    private static String TOPIC = "topic1";
    String KAFKA_SERVER_URL = "localhost";
    int KAFKA_SERVER_PORT = 9092;
    int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    int CONNECTION_TIMEOUT = 100000;
    int RECONNECT_INTERVAL = 10000;
    String TOPIC2 = "topic2";
    String TOPIC3 = "topic3";
    String CLIENT_ID = "SimpleConsumerDemoClient";

    public static void main(String[] args) {
        final boolean isAsync = args.length > 0 ? !args[0].trim().toLowerCase().equals("sync") : true;
        KafkaProducerApp producerThread = new KafkaProducerApp(TOPIC, isAsync);
        producerThread.start();

        KafkaConsumerApp consumerThread = new KafkaConsumerApp(TOPIC);
        consumerThread.start();

    }
}
