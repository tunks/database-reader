/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.reader;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import java.util.Map;
import java.util.Properties;

/**
 * Reader Factory class
 * @author ebrimatunkara
 */
public class ReaderFactory {
     public BinaryLogClient createBinLogClient(Map<String,String> config){
            String hostname = config.get("hostname");
            String schema = config.get("schema");
            String username = config.get("username");
            String password = config.get("password");
            int port = Integer.parseInt(config.get("port"));
            BinaryLogClient client = new BinaryLogClient(hostname, port, username, password);
           return client;
     }
}
