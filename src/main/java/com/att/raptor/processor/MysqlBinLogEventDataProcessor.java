/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * MySqlBinLogEvent processor
 *
 * @author ebrimatunkara
 */
public class MysqlBinLogEventDataProcessor extends EventDataProcessor {
//    private final Set<EventType> filteredEvenTypes;
//    public MysqlBinLogEventDataProcessor(Set<EventType> filteredEvenTypes) {
//        super();
//        this.filteredEvenTypes = filteredEvenTypes;
//        
//    }

    /**
     * Process data
     * @param event
     * @return
     */
    @Override
    public String processData(Event event) {
        //TODO use blocking queue here
        //Get event header
        EventHeader header = event.getHeader();
        EventType eventType = header.getEventType();
        TableMapEventData tableMap = event.getData();
        //For INSERT operations
        if (EventType.isWrite(eventType)) {
            WriteRowsEventData data = event.getData();
           
           
            processRows(tableMap, data.getIncludedColumns(), data.getRows());
        } //For UPDATE operations
        else if (EventType.isUpdate(eventType)) {
            UpdateRowsEventData data = event.getData();
        } //For DELETE operation
        else if (EventType.isDelete(eventType)) {
            DeleteRowsEventData data = event.getData();
        }
        return null;
    }
    
    private void processRows( TableMapEventData tableMap, BitSet columns, List<Serializable[]> rows  ){
          tableMap.getColumnMetadata();
          tableMap.getColumnTypes();
          //ColumnType
          EventDataDeserializer dz;
          Iterator<Serializable[]> itr = rows.iterator();
          while(itr.hasNext()){
              
            Serializable[] items = itr.next();
             //columns.get(0)
          
          }
    }

}
