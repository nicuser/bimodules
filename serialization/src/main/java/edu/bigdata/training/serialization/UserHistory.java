/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.serialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.user.post.summary.UserPost;
import org.user.post.summary.UserPostSummary;

// http://stackoverflow.com/questions/14735498/avro-avdl-file-generation
// http://stackoverflow.com/questions/21539113/can-i-split-an-apache-avro-schema-across-multiple-files
/**
 *
 * @author lsamud001c
 */
public class UserHistory extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "postHistory");
        job.setJarByClass(UserHistory.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job, new Path("input/posts/user_info.txt"), TextInputFormat.class, UserCityMapper.class);
        MultipleInputs.addInputPath(job, new Path("input/posts/user_posts.txt"), TextInputFormat.class, UserPostsMapper.class);

        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, UserPostSummary.getClassSchema());
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        Path outPath = new Path("output/user/posts");
        FileOutputFormat.setOutputPath(job, outPath);
        job.setReducerClass(UserPostHistory.class);
        //outPath.getFileSystem(job.getConfiguration()).delete(outPath, true);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UserCityMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("[,]");
            context.write(new Text(fields[0].trim()), new Text("USER-PROFILE~" + fields[1]));
        }
    }

    public static class UserPostsMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("[,]");
            context.write(new Text(fields[0].trim()), new Text("USER-POSTS~" + fields[1]));
        }
    }

    public static class UserPostHistory extends
            Reducer<Text, Text, AvroKey<CharSequence>, AvroValue<UserPostSummary>> {

        public void reduce(Text key, Iterable values,
                Reducer.Context context) throws IOException, InterruptedException {
            UserPostSummary summary = new UserPostSummary();
            summary.setPosts(new ArrayList<UserPost>());
            Iterator<Text> userInfo = values.iterator();
            while (userInfo.hasNext()) {
                String[] sourceInfo = userInfo.next().toString().split("[~]");
                if ("USER-PROFILE".equals(sourceInfo[0])) {
                    summary.setUserId(key.toString());
                    summary.setUserCity(sourceInfo[1]);
                } else if ("USER-POSTS".equals(sourceInfo[0])) {
                    summary.setTotalPosts(0);
                    for (int i = 1; i < sourceInfo.length; i++) {
                        UserPost post = new UserPost();
                        post.setPostCount(0);
                        post.setPostDate(sourceInfo[i]);
                        summary.getPosts().add(post);
                    }
                }
            }
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<UserPostSummary>(summary));
        }
    }

    public static void main(String[] args) throws Exception {
        int ecode = ToolRunner.run(new UserHistory(), args);
        System.exit(ecode);
    }
}
