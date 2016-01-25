package edu.bigdata.training.pig;

import java.io.IOException;
import org.apache.pig.PigServer;

/**
 *
 * @author lsamud001c
 */
public class GenderUdfExample {
    
    public static void main(String a[]) throws Exception {
        PigServer pigServer = new PigServer("local");

        try {
            //pigServer.registerJar("udfRegistry/pigUdf-1.0-SNAPSHOT.jar");
            runMyQuery(pigServer, "src/main/resources/population.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void runMyQuery(PigServer pigServer, String inputFile) throws IOException {
        pigServer.registerQuery("register 'udfRegistry/pigUdf-1.0-SNAPSHOT.jar';");
        pigServer.registerQuery("A = load '" + inputFile + "' using PigStorage(';') as (client_id:chararray, dob:chararray, district_id:chararray);");
        pigServer.registerQuery("B = foreach A generate flatten(GENDER(client_id,dob,district_id));");
        pigServer.store("B", "myoutput");
    }
}
