/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.hive;

/**
 *
 * @author myhome
 */
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ReverseString extends UDF {

    public Text evaluate(final Text text) {
        // return NULL if value of input is NULL.
        if (text == null) {
            return null;
        }
        // Convert Hadoop Text object to StringBuilder
        StringBuilder stringBuilder = new StringBuilder(text.toString());
        // Derive reverse of string using inbuilt api of StringBuilder
        String reverse = stringBuilder.reverse().toString();
        // Convert String to Hadoop Text object and return that.
        return new Text(reverse);
    }
}
