/*** data reader simple application to reader data from mysql bin log and post to kafka ***/
package com.att.raptor.reader;

import com.github.shyiko.mysql.binlog.event.Event;

/**
 * MySQL Binlog reader implementation class
 * @author ebrimatunkara
 */
public class MysqlBinLogReader implements IReader<Event>{
    public MysqlBinLogReader(){
    
    }

    @Override
    public void read(Event event) {
           System.out.println(" event header:: "+event.getHeader());
          System.out.println(" event data:: "+event.getData());
    }
}
