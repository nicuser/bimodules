/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.pig;

import java.io.IOException;
import org.apache.pig.PigServer;

/**
 *
 * @author myhome
 */
/**
 * The Query Phrase Popularity script (script1-local.pig or script1-hadoop.pig)
 * processes a search query log file from the Excite search engine and finds
 * search phrases that occur with particular high frequency during certain times
 * of the day.
 *
 * Reference : http://pig.apache.org/docs/r0.15.0/start.html#pig-tutorial-files
 */
public class QueryPhrasePopularity {

    public static void main(String a[]) throws Exception {
        PigServer pigServer = new PigServer("local");

        try {
            runMyQuery(pigServer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void runMyQuery(PigServer pigServer) throws IOException {
        //Register the JAR file so that the included UDFs can be called in the script.
        pigServer.registerQuery("register 'udfRegistry/pigUdf-1.0-SNAPSHOT.jar';");
        //Use the PigStorage function to load the excite log file (excite-small.log) into the “raw” bag as an array of records with the fields user, time, and query.
        pigServer.registerQuery("raw = LOAD 'src/main/resources/data/excite-small.log' USING PigStorage('\\t') AS (user, time, query);");
        //Call the NonURLDetector UDF to remove records if the query field is empty or a URL.
        pigServer.registerQuery("clean1 = FILTER raw BY org.apache.pig.builtin.NonURLDetector(query);");
        //Call the ToLower UDF to change the query field to lowercase.
        pigServer.registerQuery("clean2 = FOREACH clean1 GENERATE user, time, org.apache.pig.builtin.ToLower(query) as query;");
        //Because the log file only contains queries for a single day, we are only interested in the hour. The excite query log timestamp format is YYMMDDHHMMSS. Call the ExtractHour UDF to extract the hour (HH) from the time field.
        pigServer.registerQuery("houred = FOREACH clean2 GENERATE user, org.apache.pig.builtin.ExtractHour(time) as hour, query;");
        //Call the NGramGenerator UDF to compose the n-grams of the query.
        pigServer.registerQuery("ngramed1 = FOREACH houred GENERATE user, hour, flatten(org.apache.pig.builtin.NGramGenerator(query)) as ngram;");
        //Use the DISTINCT operator to get the unique n-grams for all records.
        pigServer.registerQuery("ngramed2 = DISTINCT ngramed1;");
        //Use the GROUP operator to group records by n-gram and hour.
        pigServer.registerQuery("hour_frequency1 = GROUP ngramed2 BY (ngram, hour);");
        //Use the COUNT function to get the count (occurrences) of each n-gram.
        pigServer.registerQuery("hour_frequency2 = FOREACH hour_frequency1 GENERATE flatten($0), COUNT($1) as count;");
        //Use the GROUP operator to group records by n-gram only. Each group now corresponds to a distinct n-gram and has the count for each hour.
        pigServer.registerQuery("uniq_frequency1 = GROUP hour_frequency2 BY group::ngram;");
        //For each group, identify the hour in which this n-gram is used with a particularly high frequency. Call the ScoreGenerator UDF to calculate a "popularity" score for the n-gram.
        pigServer.registerQuery("uniq_frequency2 = FOREACH uniq_frequency1 GENERATE flatten($0), flatten(org.apache.pig.builtin.ScoreGenerator($1));");
        //Use the FOREACH-GENERATE operator to assign names to the fields.
        pigServer.registerQuery("uniq_frequency3 = FOREACH uniq_frequency2 GENERATE $1 as hour, $0 as ngram, $2 as score, $3 as count, $4 as mean;");
        //Use the FILTER operator to remove all records with a score less than or equal to 2.0.
        pigServer.registerQuery("filtered_uniq_frequency = FILTER uniq_frequency3 BY score > 2.0;");
        //Use the ORDER operator to sort the remaining records by hour and score.
        pigServer.registerQuery("ordered_uniq_frequency = ORDER filtered_uniq_frequency BY hour, score;");
        //Use the PigStorage function to store the results. The output file contains a list of n-grams with the following fields: hour, ngram, score, count, mean.
        pigServer.store("ordered_uniq_frequency", "myoutput");
    }
}
