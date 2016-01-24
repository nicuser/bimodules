package edu.bigdata.training.hdfs;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author myhome
 */
public class HdfsApp {

    public static void main(String[] argv) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:8020");
        conf.set("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);

        // Hadoop DFS deals with Path
        Path inFile = new Path("/tmp/sample.txt");
        Path outFile = new Path("/user/root/demo");

        if (fs.exists(outFile)) {
            System.out.println("Output already exists");
            fs.delete(outFile);
        }

        // Read from and write to new file
        fs.copyFromLocalFile(inFile, outFile);
    }

}
