/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.fileformats.compress;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 *
 * @author myhome
 */
public class MapFileWriter {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, URISyntaxException {

        Configuration conf = new Configuration();
        FileSystem fs;

        try {
            fs = FileSystem.get(conf);

            URI inputURI = MapFileWriter.class.getClassLoader().getResource("sample.txt").toURI();
            Path inputFile = new Path(inputURI);
            Path outputFile = new Path("mapfile");

            Text txtKey = new Text();
            Text txtValue = new Text();

            MapFile.Writer writer = null;

            FSDataInputStream inputStream = fs.open(inputFile);

            try {
                System.out.println(outputFile.toString());
                writer = new MapFile.Writer(conf, fs, outputFile.toString(),
                        txtKey.getClass(), txtKey.getClass());
                writer.setIndexInterval(1);//Need this as the default is 128, and my data is just 9 records
                DataInputStream stream = new DataInputStream(inputStream);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                String strLineInInputFile = null;
                int line = 1;
                while ((strLineInInputFile = reader.readLine()) != null) {
                    String lstKeyValuePair[] = strLineInInputFile.split(",");
                    txtKey.set(lstKeyValuePair[0]);
                    txtValue.set(lstKeyValuePair[1]);
                    writer.append(txtKey, txtValue);
                }
            } finally {
                IOUtils.closeStream(writer);
                System.out.println("Map file created successfully!!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
