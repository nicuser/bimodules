package edu.bigdata.training.crunch;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author myhome
 */
public class WordCount extends Configured implements Tool, Serializable {

    public int run(String[] args) throws Exception {
        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(WordCount.class, getConf());
        // Reference a given text file as a collection of Strings.
        PCollection<String> lines = pipeline.readTextFile("/user/root/employees/part-m-00000");

        // Define a function that splits each line in a PCollection of Strings into
        // a
        // PCollection made up of the individual words in the file.
        PCollection<Long> numberOfWords = lines.parallelDo(new DoFn<String, Long>() {
            public void process(String line, Emitter<Long> emitter) {
                emitter.emit((long) line.split("\\s+").length);
            }
        }, Writables.longs()); // Indicates the serialization format

        // The aggregate method groups a collection into a single PObject.
        PObject<Long> totalCount = numberOfWords.aggregate(Aggregators.SUM_LONGS()).first();

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.run();

        System.out.println("Total number of words: " + totalCount.getValue());

        pipeline.done();

        return result.succeeded() ? 0 : 1;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(result);
    }
}
