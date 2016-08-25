/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.bigdata.training.fileformats.avro;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import static org.apache.hadoop.io.SequenceFile.Reader.file;

/**
 *
 * @author myhome
 */
public class TradeFactory {

    public Trade getTrade() {
        return Trade.newBuilder().setStockSymbol("SYMBOL")
                .setTradePrice(123)
                .setTradeTime(234)
                .build();
    }

    public void saveTrade() throws IOException {
        Trade trade = getTrade();
        DatumWriter<Trade> tradeDatumWriter = new SpecificDatumWriter<Trade>(Trade.class);
        DataFileWriter<Trade> dataFileWriter = new DataFileWriter<Trade>(tradeDatumWriter);
        dataFileWriter.create(trade.getSchema(), new File("trades.avro"));
        dataFileWriter.append(trade);
        dataFileWriter.close();
    }

    public void loadTrade() throws IOException {
        DatumReader<Trade> userDatumReader = new SpecificDatumReader<Trade>(Trade.class);
        File tradesInfo = new File("trades.avro");
        DataFileReader<Trade> dataFileReader = new DataFileReader<Trade>(tradesInfo, userDatumReader);
        Trade trade = null;
        while (dataFileReader.hasNext()) {
            trade = dataFileReader.next(trade);
            System.out.println(trade);
        }
    }

    public static void main(String a[]) throws IOException, InterruptedException {
        TradeFactory factory = new TradeFactory();
        factory.saveTrade();
        factory.loadTrade();
    }
}
