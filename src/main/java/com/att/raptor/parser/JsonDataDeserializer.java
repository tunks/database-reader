/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.parser;

import com.att.raptor.processor.MysqlBinLogEventDataHandler.RowData;
import com.att.raptor.processor.MysqlBinLogEventDataHandler.RowDatabaseTable;
import com.att.raptor.reader.SchemaRetriever;
import com.att.raptor.reader.SchemaTree;
import com.att.raptor.reader.SchemaTree.TableColumn;
import com.att.raptor.reader.SchemaTree.TableMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 *
 * @author ebrimatunkara
 */
public class JsonDataDeserializer implements DataDeserializer<RowData, List> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SchemaTree schemaTree;
    private final SchemaRetriever schemaRetriever;
    private final String FieldValueSeperator = ":";
    private final String Quote = "\"";

    public JsonDataDeserializer(SchemaRetriever schemaRetriever) {
        this.schemaRetriever = schemaRetriever;
        schemaTree = schemaRetriever.getSchemaTree();
    }

    //private     
    @Override
    public List deserialize(RowData data) {
        List<String> list = new ArrayList();
        RowDatabaseTable dbTable = data.getDatabaseTable();
        TableMap tableMap = schemaTree.findTableByName(dbTable.getTable());
        if (tableMap != null) {
            Set<TableColumn> columns = tableMap.getColumns();
            Iterator<Serializable[]> itr = data.getRows().iterator();
            while (itr.hasNext()) {
                //Deserialize row data to json 
                list.add(deserializeRowDataToJson(columns, itr.next()));
            }
        }
        return list;
    }

    /**
     * Deserialize row data to json
     */
    private String deserializeRowDataToJson(Set<TableColumn> columns, Serializable[] items) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        ItemSeperator seperator = new ItemSeperator();
        columns.stream().forEach((TableColumn column) -> {
            try {
                Serializable value = items[column.getIndex() - 1];
                //append  seperator which is empty  string by default
                builder.append(seperator);
                builder.append(Quote);
                builder.append(column.getName())
                        .append(Quote)
                        .append(FieldValueSeperator)
                        .append(formatColumnValue(column.getType(), value)); //format and append column value
                //append comma seperator
                seperator.setSeperator(",");

            } catch (IndexOutOfBoundsException ex) {
                System.out.println(ex.getMessage());
            }
        });

        builder.append("}");
        return builder.toString();
    }

    /**
     * Format column value
     */
    private Serializable formatColumnValue(String columnType, Serializable value) {
        StringBuilder builder = new StringBuilder();
        switch (columnType) {
            case "VARCHAR":
            case "LONGVARCHAR":
            case "CHAR":
            case "LONGVARBINARY":
            case "DATE":
                builder.append(Quote)
                        .append(value)
                        .append(Quote);
                break;
            default:
                builder.append(value);
                break;
        }
        return builder.toString();
    }

    private class ItemSeperator implements Serializable {

        private String seperator = "";

        public String getSeperator() {
            return seperator;
        }

        public void setSeperator(String seperator) {
            this.seperator = seperator;
        }

        @Override
        public String toString() {
            return seperator;
        }

    }

}
