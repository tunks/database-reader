/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.reader;

import com.att.raptor.processor.ClientProcessor;
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
public class ReaderFactoryTest {
    private ReaderFactory readerFactory;
    public ReaderFactoryTest() {
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
        config.put("hostname","10.20.77.129");
        config.put("schema","test");
        config.put("username","sanRead");
        config.put("password","sandeep");
        config.put("port", "3306");
        readerFactory = new ReaderFactory(config);
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of createBinLogClient method, of class ReaderFactory.
     */
//    @Test
//    public void testCreateBinLogClient() {
//        System.out.println("createBinLogClient");
//        Map<String, String> config = null;
//        ReaderFactory instance = new ReaderFactory();
//        BinaryLogClient expResult = null;
//        BinaryLogClient result = instance.createBinLogClient(config);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }

    /**
     * Test of createSchemaRetriever method, of class ReaderFactory.
     */
//    @Test
//    public void testCreateSchemaRetriever() {
//        System.out.println("createSchemaRetriever");
//        SchemaRetriever result = readerFactory.createSchemaRetriever();
//        assertNotNull(result);
//        SchemaTree tree =  result.getSchemaTree();
//        System.out.println(tree);
//        assertNotNull(tree);
//       
//    }
    
      /**
     * Test of CreateClientProcessor method, of class ReaderFactory.
     */
    @Test
    public void testCreateClientProcessor(){
        System.out.println("CreateClientProcessor");
        ClientProcessor clientProcessor = readerFactory.createClientProcessor();
        assertNotNull(clientProcessor);
        clientProcessor.process();
       
    }
    
}
