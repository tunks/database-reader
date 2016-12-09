/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.reader;

/**
 *
 * @author ebrimatunkara
 */
public interface SchemaRetriever {
   public SchemaTree getSchemaTree();      
}
