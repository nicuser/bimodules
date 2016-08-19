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
// Make sure docker host name mapped to ip address in /etc/hosts
public class KafkaConsumerProducerDemo {
    private static String TOPIC = "Hello-Kafka";

    public static void main(String[] args) {
        final boolean isAsync = args.length > 0 ? !args[0].trim().toLowerCase().equals("sync") : true;
        KafkaProducerApp producerThread = new KafkaProducerApp(TOPIC, isAsync);
        producerThread.start();

//        KafkaConsumerApp consumerThread = new KafkaConsumerApp(TOPIC);
//        consumerThread.start();

    }
}
