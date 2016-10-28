/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.att.raptor.reader.ReaderFactory;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mysql client processor
 * @author ebrimatunkara
 */
public class MysqlClientProcessor implements ClientProcessor{
    private final ReaderFactory factory  = new ReaderFactory();
    private final BinaryLogClient  client;
    private final  BinaryLogClient.EventListener   eventListener;
    public MysqlClientProcessor(Map<String,String> config ,  BinaryLogClient.EventListener  eventListener) {
         //this.config = config;
        client = factory.createBinLogClient(config);
        this.eventListener = eventListener;
        //TODO bin log position
    }
     
    @Override
    public void process() {
        try {
            //register listener and start processing data
            client.registerEventListener(eventListener);
            client.setBinlogFilename("");
            client.setBinlogPosition(4);
            client.connect();
            
          System.out.println(" test binlog " +client.getBinlogPosition());
        } catch (IOException ex) {
            Logger.getLogger(MysqlClientProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
