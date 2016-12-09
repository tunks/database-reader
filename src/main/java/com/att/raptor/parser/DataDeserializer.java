/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.parser;

/**
 *
 * @author ebrimatunkara
 * @param <T>
 * @param <V>
 */
public interface DataDeserializer<T,V> {
      public V deserialize(T data);
}
