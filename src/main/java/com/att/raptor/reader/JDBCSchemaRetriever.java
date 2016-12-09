package com.att.raptor.reader;

import com.att.raptor.reader.SchemaTree.TableColumn;
import com.att.raptor.reader.SchemaTree.TableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * Created by sandeep on 12/8/16.
 */
public class JDBCSchemaRetriever implements  SchemaRetriever{
    private final Connection  connection;
    public JDBCSchemaRetriever(Connection connection) {
        this.connection = connection;
    }
    
    private SchemaTree createSchemaTree() throws SQLException {
        SchemaTree schemaTree = new SchemaTree();
        if (connection != null) {
            System.out.println("You made it, take control your database now!");

            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tableResultSet = metaData.getTables(null, "public", "%", new String[]{"TABLE"})) {
                while (tableResultSet.next()) {
                    Set<TableColumn> columns = new HashSet();
                    String tableName = tableResultSet.getString("TABLE_NAME");
                    
                    TableMap tableMap =  new TableMap(tableName);
                    try (ResultSet columnResultSet = metaData.getColumns(null, "public", tableName,  "%")) {
                        while (columnResultSet.next()) {
                            String columnName = columnResultSet.getString("COLUMN_NAME");
                            String columnType = columnResultSet.getString("TYPE_NAME");
                            int columnIndex = columnResultSet.getInt("ORDINAL_POSITION");
                            columns.add(new TableColumn(columnName,columnType.toUpperCase(),columnIndex));
                        }
                    }
                   tableMap.setColumns(columns);
                   schemaTree.addTableMap(tableMap);
                }
            } 
        } else {
            System.out.println("Failed to make connection!");
        }
       return schemaTree;
    } 

    @Override
    public SchemaTree getSchemaTree() {
        try {
            return createSchemaTree();
        } catch (SQLException ex) {
            Logger.getLogger(JDBCSchemaRetriever.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}
