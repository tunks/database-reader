/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.github.shyiko.mysql.binlog.event.Event;

/**
 * DataProcessor
 * @author ebrimatunkara
 */
public abstract class EventDataProcessor implements DataProcessor<Event, String> {
    private EventDataProcessor dataProcessor;

    public EventDataProcessor() {
    }

    public EventDataProcessor(EventDataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    public EventDataProcessor getDataProcessor() {
        return dataProcessor;
    }

    public void setDataProcessor(EventDataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

}
