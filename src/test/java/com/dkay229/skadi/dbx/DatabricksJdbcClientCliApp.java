package com.dkay229.skadi.dbx;

import java.sql.*;
import java.util.Properties;

/**
 * Simple CLI app to test Databricks JDBC connectivity.
 * Expects the JDBC URL in the DBX_JDBC_URL environment variable.
 * Usage:
 *<pre>
 * export DBX_JDBC_URL="jdbc:databricks://<your-databricks-instance>?...&accessToken=<your-access-token>"
 * java -cp <your-classpath-including-databricks-jdbc-driver> com.dkay229.skadi.dbx.DatabricksJdbcClientCliApp
 *
 * Powershell:
 * $env:DBX_JDBC_URL="jdbc:databricks://...;httpPath=...;AuthMech=3;UID=token;PWD=..."
 *
 * Build your JDBC URL
 *
 * Databricks will often show you a JDBC URL on the Connection details tab (best: copy/paste that).
 * Databricks Documentation
 *
 * If you need to construct it, a very common SQL Warehouse pattern is:
 *
 * jdbc:databricks://<SERVER_HOSTNAME>:443/default;transportMode=http;ssl=1;httpPath=<HTTP_PATH>;AuthMech=3;UID=token;PWD=<PAT>
 *
 *
 * This token-based auth style is widely used with the Databricks JDBC driver.
 * Databricks Documentation
 * +2
 * Databricks Community
 * +2
 *
 * Example (shape only)
 * jdbc:databricks://dbc-xxxx.cloud.databricks.com:443/default;
 * transportMode=http;ssl=1;
 * httpPath=/sql/1.0/warehouses/xxxx;
 * AuthMech=3;UID=token;PWD=dapi....
 *
 *</pre>
 *
 */
public class DatabricksJdbcClientCliApp {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:databricks://dbc-ccf13444-2e7e.cloud.databricks.com:443;HttpPath=/sql/1.0/warehouses/08521a3bfdd61498";
        Properties properties = new Properties();
        // dapi644555d5403b91fbe6f5458e3ba7928b
        //properties.put("PWD",);
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            if (connection != null) {
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW SCHEMAS");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString("databaseName"));
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
