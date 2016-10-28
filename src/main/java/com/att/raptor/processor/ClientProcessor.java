package com.att.raptor.processor;

/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */


/**
 *  Data processor
 * @author ebrimatunkara
 */
public interface ClientProcessor {
    public void process();
}
