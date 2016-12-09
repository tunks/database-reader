/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mysql client processor
 * @author ebrimatunkara
 */
public class MysqlClientProcessor implements ClientProcessor{
    private final BinaryLogClient  client;
    public MysqlClientProcessor(BinaryLogClient client) {
         //this.config = config;
        this.client = client;
        //TODO bin log position
    }
     
    @Override
    public void process() {
        try {
            //register listener and start processing data
            client.setBinlogFilename("");
            client.setBinlogPosition(4);
            client.connect();
            
            
          System.out.println(" test binlog " +client.getBinlogPosition());
        } catch (IOException ex) {
            Logger.getLogger(MysqlClientProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
