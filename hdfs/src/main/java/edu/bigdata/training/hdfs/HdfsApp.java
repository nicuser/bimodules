package edu.bigdata.training.hdfs;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author myhome
 */
// for more details refer to section "How to write file into HDFS from outside of hadoop cluster" in BigdataDemo document
// https://community.hortonworks.com/questions/16837/cannot-copy-from-local-machine-to-vm-datanode-via.html

public class HdfsApp {

    public static void main(String[] argv) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Hadoop DFS deals with Path
        Path inFile = new Path("src/main/resources/sample.txt");
        Path outFile = new Path("/user/myhome/sample");

        if (fs.exists(outFile)) {
            System.out.println("Output already exists");
            fs.delete(outFile);
        }

        // Read from and write to new file
        fs.copyFromLocalFile(inFile, outFile);
        System.exit(0);
    }

}
