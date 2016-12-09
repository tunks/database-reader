/*** data reader simple application to reader data from mysql bin log and post to kafka ***/
package com.att.raptor.reader;

import com.att.raptor.processor.DataHandler;
import com.github.shyiko.mysql.binlog.event.Event;

/**
 * MySQL Binlog reader implementation class
 * @author ebrimatunkara
 */
public class MysqlBinLogReader implements IReader<Event>{
    private final DataHandler handler;

    public MysqlBinLogReader(DataHandler handler) {
        this.handler = handler;
    }
 

    @Override
    public void read(Event event) {
          handler.handlerData(event);
    }
}
