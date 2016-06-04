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
//before running this make sure your core-site.xml fs.defaultFS pointing to 0.0.0.0:8020
// Permissions : http://stackoverflow.com/questions/11593374/permission-denied-at-hdfs
public class HdfsApp {

    public static void main(String[] argv) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.99.100:8020");
        conf.set("HADOOP_USER_NAME", "cloudera");
        FileSystem fs = FileSystem.get(conf);

        // Hadoop DFS deals with Path
        Path inFile = new Path("src/main/resources/sample.txt");
        Path outFile = new Path("/user/cloudera/sample");

        if (fs.exists(outFile)) {
            System.out.println("Output already exists");
            fs.delete(outFile);
        }

        // Read from and write to new file
        fs.copyFromLocalFile(inFile, outFile);
        System.exit(0);
    }

}
