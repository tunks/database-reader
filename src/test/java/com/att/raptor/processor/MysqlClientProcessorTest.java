/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.att.raptor.reader.IReader;
import com.att.raptor.reader.MysqlBinLogReader;
import com.github.shyiko.mysql.binlog.event.Event;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ebrimatunkara
 */
public class MysqlClientProcessorTest {
    private ClientProcessor clientProcessor;
    
    public MysqlClientProcessorTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        Map<String,String> config = new HashMap();
        config.put("hostname","10.20.75.181");
        config.put("schema","test");
        config.put("username","sanRead");
        config.put("password","sandeep");
        config.put("port", "3306");
        IReader<Event> reader = new MysqlBinLogReader();
        clientProcessor = ProcessorFactory.createClientProcessor(config,reader);
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of process method, of class MysqlClientProcessor.
     */
    @Test
    public void testProcess() {
       clientProcessor.process();
       System.out.println("hold here");
    }
    
}
