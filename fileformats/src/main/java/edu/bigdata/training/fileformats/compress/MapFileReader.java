/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.fileformats.compress;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author myhome
 */
public class MapFileReader {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = null;
        Text txtKey = new Text("d005");
        Text txtValue = new Text();
        MapFile.Reader reader = null;

        try {
            fs = FileSystem.get(conf);

            try {
                reader = new MapFile.Reader(fs, "src/main/resources/mapfile", conf);
                reader.get(txtKey, txtValue);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        System.out.println("The key is " + txtKey.toString()
                + " and the value is " + txtValue.toString());
    }
}
