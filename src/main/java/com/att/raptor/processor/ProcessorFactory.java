/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.att.raptor.listener.MysqlEventListener;
import com.att.raptor.reader.IReader;
import java.util.Map;

/**
 * Message Processor Factory
 * @author ebrimatunkara
 */
public class ProcessorFactory {
    public  static ClientProcessor createClientProcessor(Map<String,String> config, IReader reader){
        return new MysqlClientProcessor(config, new MysqlEventListener(reader));
    }
}
