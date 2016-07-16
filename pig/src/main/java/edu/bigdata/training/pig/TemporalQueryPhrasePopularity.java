package edu.bigdata.training.pig;

import java.io.IOException;
import org.apache.pig.PigServer;

/**
 * The Temporal Query Phrase Popularity script processes a search query 
 * log file from the Excite search engine and compares the occurrence of frequency 
 * of search phrases across two time periods separated by twelve hours.
 * 
 */
public class TemporalQueryPhrasePopularity {

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
        //Use the FOREACH-GENERATE operator to assign names to the fields.
        pigServer.registerQuery("hour_frequency3 = FOREACH hour_frequency2 GENERATE $0 as ngram, $1 as hour, $2 as count;");
        //Use the FILTERoperator to get the n-grams for hour ‘00’
        pigServer.registerQuery("hour00 = FILTER hour_frequency2 BY hour eq '00';");
        //Uses the FILTER operators to get the n-grams for hour ‘12’
        pigServer.registerQuery("hour12 = FILTER hour_frequency3 BY hour eq '12';");
        //Use the JOIN operator to get the n-grams that appear in both hours.
        pigServer.registerQuery("same = JOIN hour00 BY $0, hour12 BY $0;");
        //Use the FOREACH-GENERATE operator to record their frequency.
        pigServer.registerQuery("same1 = FOREACH same GENERATE hour00::group::ngram as ngram, $2 as count00, $5 as count12;");
        //Use the PigStorage function to store the results. The output file contains a list of n-grams with the following fields: ngram, count00, count12.
        pigServer.store("same1", "myoutput");
    }
}
