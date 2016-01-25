package edu.bigdata.training.pig;

/**
 *
 * @author lsamud001c
 */
import java.io.IOException;
import org.apache.pig.PigServer;

public class EmbeddedPigScript {

    public static void main(String[] args) {
        try {
            PigServer pigServer = new PigServer("local");
            runIdQuery(pigServer, "src/main/resources/passwd");
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    public static void runIdQuery(PigServer pigServer, String inputFile) throws IOException {
        pigServer.registerQuery("A = load '" + inputFile + "' using PigStorage(':');");
        pigServer.registerQuery("B = foreach A generate $0 as id;");
        pigServer.store("B", "id.out");
    }
}
