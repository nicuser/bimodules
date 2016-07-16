/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.pig;

/**
 *
 * @author lsamud001c
 */
import java.io.IOException;
import org.apache.pig.PigServer;

public class EmbeddedSample {

    public static void main(String a[]) throws Exception {
        PigServer pigServer = new PigServer("local");

        try {
            runMyQuery(pigServer, "src/main/resources/myinput.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void runMyQuery(PigServer pigServer, String inputFile) throws IOException {
        pigServer.registerQuery("A = load '" + inputFile + "' using TextLoader();");
        pigServer.registerQuery("B = foreach A generate flatten(TOKENIZE($0));");
        pigServer.registerQuery("C = group B by $0;");
        pigServer.registerQuery("D = foreach C generate flatten(group), COUNT(B.$0);");
        pigServer.registerQuery("E = FILTER D BY $1 > 3;");
        
        pigServer.store("E", "myoutput");
    }
}
