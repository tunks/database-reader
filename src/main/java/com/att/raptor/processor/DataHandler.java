/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

/**
 * DataProcessor
 * @author ebrimatunkara
 * @param <T>
 * @param <V>
 */
public interface DataHandler<T,V> {
    public void handlerData(T data);
}
