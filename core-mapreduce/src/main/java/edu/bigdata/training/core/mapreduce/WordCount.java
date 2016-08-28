/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.core.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author myhome
 */
public class WordCount {
    private enum METRICS {
        TOTAL_WORDS
    }

    public static class SimpleMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1); // here it is count of the word , for every repeated word it will add one
        private Text word = new Text(); // here it will save the word and checks if the word repeats

        @Override
        public void map(LongWritable key, Text Value, Context context) throws IOException, InterruptedException {
            System.out.println(" key is " + key);

            String line = Value.toString();
            StringTokenizer st = new StringTokenizer(line); // this will break the line into tokens
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
                context.getCounter(METRICS.TOTAL_WORDS).increment(1L);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        // here now input will be in form of like Aditya --> Text as key  1,1,1,1 -- value
        // Iterable , this will iterate through the values like 1,1,1,1 which are values
        public void reduce(Text Key, Iterable<IntWritable> Value, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // now we will use the for each loop to iterate over and over 
            for (IntWritable val : Value) {
                sum = sum + val.get();
            }
            // so for particular Key it will show the added values in the total
            // so it will be Aditya - KEY value -> 4
            context.write(Key, new IntWritable(sum));
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        {

            System.out.println("arg[0]-->" + args[0]);
            System.out.println("arg[1]-->" + args[1]);

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(SimpleMapper.class);
            job.setCombinerClass(Reduce.class);
            job.setReducerClass(Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            
            System.out.println("Total Words:"+job.getCounters().findCounter(METRICS.TOTAL_WORDS).getValue());
        }
    }
}
