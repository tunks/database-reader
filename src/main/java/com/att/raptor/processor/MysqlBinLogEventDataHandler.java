/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.processor;

import com.att.raptor.parser.DataDeserializer;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * MySqlBinLogEvent processor
 *
 * @author ebrimatunkara
 */
public class MysqlBinLogEventDataHandler extends EventDataHandler {

    private final ConcurrentMap<Long, RowDatabaseTable> tableCache;

    public MysqlBinLogEventDataHandler(BlockingQueue queue, DataDeserializer deserializer) {
        super(queue, deserializer);
        tableCache = new ConcurrentHashMap();
    }

    @Override
    public void handlerData(Event event) {
        RowData rowData = null;
        EventHeaderV4 header = event.getHeader();
        EventType eventType = header.getEventType();
        //For INSERT operations
        if (EventType.isWrite(eventType)) {
            WriteRowsEventData data = event.getData();
            rowData = new RowDataBuilder().setTableId(data.getTableId())
                    .setRows(data.getRows())
                    .setColumns(getIncludedColumns(data.getIncludedColumns()))
                    .setPosition(header.getPosition())
                    .setDatabaseTable(tableCache.get(data.getTableId()))
                    .build();
        } //For UPDATE operations
        else if (EventType.isUpdate(eventType)) {
            UpdateRowsEventData data = event.getData();
            rowData = new RowDataBuilder().setTableId(data.getTableId())
                    .setRows(data.getRows().stream().map(x -> {
                        return x.getValue();
                    }).collect(Collectors.toList()))
                    .setPosition(header.getPosition())
                    .setColumns(getIncludedColumns(data.getIncludedColumns()))
                    .setDatabaseTable(tableCache.get(data.getTableId()))
                    .build();

        } //For DELETE operation
        else if (EventType.isDelete(eventType)) {
            DeleteRowsEventData data = event.getData();

            rowData = new RowDataBuilder().setTableId(data.getTableId())
                    .setRows(data.getRows())
                    .setPosition(header.getPosition())
                    .setColumns(getIncludedColumns(data.getIncludedColumns()))
                    .setDatabaseTable(tableCache.get(data.getTableId()))
                    .build();

        }

        //cache table name and id 
        if (event.getData() instanceof TableMapEventData) {
            TableMapEventData data = event.getData();        
            tableCache.put(data.getTableId(),new RowDatabaseTable(data.getTable(),data.getDatabase()));
        }

        if (rowData != null) {
            insertRowData(rowData);
        }
    }

    private Set<Integer> getIncludedColumns(BitSet includedColumns) {
        Set<Integer> columns = new HashSet();
        includedColumns.stream().forEach(columns::add);
        return columns;
    }

    /**
     * Row data
     */
    public static class RowData implements Serializable {

        private Long tableId;
        public List<Serializable[]> rows;
        public Set<Integer> columns;
        public Long position;
        private RowDatabaseTable databaseTable;

        public Long getTableId() {
            return tableId;
        }

        public void setTableId(Long tableId) {
            this.tableId = tableId;
        }

        public List<Serializable[]> getRows() {
            return rows;
        }

        public void setRows(List<Serializable[]> rows) {
            this.rows = rows;
        }

        public Set<Integer> getColumns() {
            return columns;
        }

        public void setColumns(Set<Integer> columns) {
            this.columns = columns;
        }

        public Long getPosition() {
            return position;
        }

        public void setPosition(Long position) {
            this.position = position;
        }

        public RowDatabaseTable getDatabaseTable() {
            return databaseTable;
        }

        public void setDatabaseTable(RowDatabaseTable databaseTable) {
            this.databaseTable = databaseTable;
        }

        @Override
        public String toString() {
            return "RowData{" + "tableId=" + tableId + ", rows=" + rows + ", columns=" + columns + ", position=" + position + ", databaseTable=" + databaseTable + '}';
        }
    }

    public static class RowDataBuilder {

        private final RowData rowData = new RowData();

        public RowDataBuilder setTableId(Long tableId) {
            rowData.setTableId(tableId);
            return this;
        }

        public RowDataBuilder setRows(List<Serializable[]> rows) {
            rowData.setRows(rows);
            return this;
        }

        public RowDataBuilder setColumns(Set<Integer> columns) {
            rowData.setColumns(columns);
            return this;
        }

        public RowDataBuilder setPosition(Long position) {
            rowData.setPosition(position);
            return this;
        }

        public RowDataBuilder setDatabaseTable(RowDatabaseTable dbTable) {
            rowData.setDatabaseTable(dbTable);
            return this;
        }

        public RowData build() {
            return rowData;
        }

    }

    public static class RowDatabaseTable {

        private String table;
        private String database;

        public RowDatabaseTable() {
        }

        public RowDatabaseTable(String table, String database) {
            this.table = table;
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 89 * hash + Objects.hashCode(this.table);
            hash = 89 * hash + Objects.hashCode(this.database);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final RowDatabaseTable other = (RowDatabaseTable) obj;
            if (!Objects.equals(this.table, other.table)) {
                return false;
            }
            if (!Objects.equals(this.database, other.database)) {
                return false;
            }
            return true;
        }

    }
}
