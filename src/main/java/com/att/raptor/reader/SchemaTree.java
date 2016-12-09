/**
 * Data reader simple application to reader data from mysql bin log and post to kafka
 *
 */
package com.att.raptor.reader;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author ebrimatunkara
 */
public class SchemaTree {

    private final Set<TableMap> tables = new HashSet();

    public Set<TableMap> getTables() {
        return tables;
    }

    public void addTableMap(TableMap table) {
        tables.add(table);
    }

    public void addTableColumn(String tableName, TableColumn column) {
        getTables().stream().filter((table) -> (table.getTableName().equals(tableName))).forEach((table) -> {
            table.addColumn(column);
        });

    }

    public void setTableId(String tableName, String tableId) {
        getTables().stream().filter((table) -> (table.getTableName().equals(tableName))).forEach((table) -> {
            table.setTableId(tableId);
        });

    }

    public TableMap findTableByName(String tableName) {
        try {
            return getTables().stream().filter((table) -> (table.getTableName().equals(tableName))).findFirst().get();
        } catch (NoSuchElementException ex) {
            //logger
            System.out.println("Find table by name " + ex.getMessage());
        }
        return null;
    }

    public TableMap findTableById(String tableId) {
        try {
            return getTables().stream().filter((table) -> (table.getTableName().equals(tableId))).findFirst().get();
        } catch (NoSuchElementException ex) {
            //logger
            System.out.println("Find table by id " + ex.getMessage());
        }
        return null;
    }

    @Override
    public String toString() {
        return "SchemaTree{" + "tables=" + tables + '}';
    }

    public static class TableMap {

        private String tableId;
        private String tableName;
        private Set<TableColumn> columns = new HashSet();

        public TableMap() {
        }

        public TableMap(String tableName) {
            this.tableName = tableName;
        }

        public TableMap(String tableId, String tableName) {
            this.tableId = tableId;
            this.tableName = tableName;
        }

        public String getTableId() {
            return tableId;
        }

        public void setTableId(String tableId) {
            this.tableId = tableId;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public Set<TableColumn> getColumns() {
            return columns;
        }

        public void setColumns(Set<TableColumn> columns) {
            this.columns = columns;
        }

        public void addColumn(TableColumn column) {
            this.columns.add(column);
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 83 * hash + Objects.hashCode(this.tableId);
            hash = 83 * hash + Objects.hashCode(this.tableName);
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
            final TableMap other = (TableMap) obj;
            if (!Objects.equals(this.tableId, other.tableId)) {
                return false;
            }
            if (!Objects.equals(this.tableName, other.tableName)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "TableMap{" + "tableId=" + tableId + ", tableName=" + tableName + ", columns=" + columns + '}';
        }

    }

    public static class TableColumn {
        private String name;
        private String type;
        private final int index;

        public TableColumn(String name, String type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public int getIndex() {
            return index;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 47 * hash + Objects.hashCode(this.name);
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
            final TableColumn other = (TableColumn) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "TableColumn{" + "name=" + name + ", type=" + type + ", index=" + index + '}';
        }
    }
}
