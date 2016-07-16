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
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Refer : "BigdataDemo" google doc refer "Hadoop with Cassandra Demo" for more instructions
//Reference : https://github.com/manum/mr-cassandra

public class MapReduceExample {

    //Mapper Class 
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        //Cassandra helper used to write data
        private CassandraHelper cclient = new CassandraHelper();

        private final static IntWritable one = new IntWritable(1);

        //Map method
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(value, one);
            cclient.addKey(line.toString());
        }

        //Sets up the Cassandra connection to be used by the mapper
        public void setup(Context context) {
            cclient.createConnection("192.168.99.100");
        }

        //Closes the Cassandra connection after the mapper is done
        public void cleanup(Context context) {
            cclient.closeConnection();
        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MR Keying");
        job.setJarByClass(MapReduceExample.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/user/root/input/all-shakespeare.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/root/output/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
