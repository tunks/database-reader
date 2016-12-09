/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.att.raptor.parser.DataDeserializer;
import com.att.raptor.processor.MysqlBinLogEventDataHandler.RowData;
import com.github.shyiko.mysql.binlog.event.Event;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DataProcessor
 *
 * @author ebrimatunkara
 */
public abstract class EventDataHandler implements DataHandler<Event, String> {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final BlockingQueue<RowData> queue;
    private final DataDeserializer deserializer;
    public EventDataHandler(BlockingQueue queue, DataDeserializer deserializer) {
        this.queue = queue;
        this.deserializer = deserializer;
        startTask();
    }

    protected void insertRowData(RowData data){
        queue.offer(data);
    }
    
    private void startTask() {
        try {
            executorService.execute(new HandlerTask());
        } catch (Exception ex) {
            executorService.shutdown();
        }
    }


    private class HandlerTask implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    RowData data = queue.poll();
                    if (data != null) {
                       Object value = deserializer.deserialize(data);
                       System.out.println("after deserialize: "+value);
                    }
                    else{  
                       Thread.sleep(1000);
                    }
                }
            } catch (InterruptedException ex) {
                Logger.getLogger(EventDataHandler.class.getName()).log(Level.SEVERE, null, ex);
            } finally{
                System.out.println("Task interruped :");
            }

        }
    }

}
