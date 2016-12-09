/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.reader;

import com.att.raptor.listener.MysqlEventListener;
import com.att.raptor.parser.DataDeserializer;
import com.att.raptor.parser.JsonDataDeserializer;
import com.att.raptor.processor.ClientProcessor;
import com.att.raptor.processor.DataHandler;
import com.att.raptor.processor.MysqlBinLogEventDataHandler;
import com.att.raptor.processor.MysqlBinLogEventDataHandler.RowData;
import com.att.raptor.processor.MysqlClientProcessor;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reader Factory class
 *
 * @author ebrimatunkara
 */
public class ReaderFactory {

    private final String DRIVER = "jdbc:mysql";
    private String hostname;
    private String username;
    private String password;
    private int port;
    private String schema;
    private Connection connection;
    private final Map<String, String> configurations;
    private final BlockingQueue<RowData> dataQueue;
  
    public ReaderFactory(Map<String, String> configurations) {
        this.dataQueue = new LinkedBlockingDeque();
        this.configurations = configurations;
        initConfigurations();
    }

    public BinaryLogClient createBinLogClient() {
        BinaryLogClient client = new BinaryLogClient(hostname, port,schema,username, password);
        return client;
    }

    public SchemaRetriever createSchemaRetriever() {
        try {
            try {
                return new JDBCSchemaRetriever(getConnection());
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(ReaderFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            Logger.getLogger(ReaderFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private ClientProcessor createClientProcessor(IReader reader) {
        BinaryLogClient client = createBinLogClient();
        client.registerEventListener(new MysqlEventListener(reader));
        return new MysqlClientProcessor(client);
    }
    
   public ClientProcessor createClientProcessor() {
         SchemaRetriever schemaRetriever = createSchemaRetriever();
         DataDeserializer deserializer = new JsonDataDeserializer(schemaRetriever);
         DataHandler handler = new MysqlBinLogEventDataHandler(dataQueue,deserializer);
         return createClientProcessor(new MysqlBinLogReader(handler));
    }


    private Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            String url = createConnectionURL();
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, username, password);
        }
        return connection;
    }

    private String createConnectionURL() {
        StringBuilder builder = new StringBuilder();
        builder.append(DRIVER)
                .append("://")
                .append(hostname)
                .append(":")
                .append(port)
                .append("/")
                .append(schema);
        return builder.toString();
    }

    private void initConfigurations() {
        hostname = configurations.get("hostname");
        schema = configurations.get("schema");
        username = configurations.get("username");
        password = configurations.get("password");
        port = Integer.parseInt(configurations.get("port"));
    }
}
