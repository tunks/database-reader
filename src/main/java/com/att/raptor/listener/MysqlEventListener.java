/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.listener;

import com.att.raptor.reader.IReader;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;

/**
 * Default MysQlEvent listener
 * @author ebrimatunkara
 */
public class MysqlEventListener implements  BinaryLogClient.EventListener {
    private final IReader<Event> reader;

    public MysqlEventListener(IReader reader) {
        this.reader = reader;
    }
    
    @Override
    public void onEvent(Event event) {
       reader.read(event);
    }
}
