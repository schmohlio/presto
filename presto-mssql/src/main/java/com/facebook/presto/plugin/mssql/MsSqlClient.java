/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.mssql;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.type.Type;
import net.sourceforge.jtds.jdbc.Driver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class MsSqlClient
        extends BaseJdbcClient
{
    protected final int isolationLevel;

    @Inject
    public MsSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, MsSqlConfig msSqlConfig)
            throws SQLException
    {
        super(connectorId, config, "\"", new Driver()); // https://msdn.microsoft.com/en-us/library/ms174393.aspx

        // bring msSqlConfig properties into current scope, since jTDS does not implement all connection string params.
        isolationLevel = msSqlConfig.getIsolationLevel();

        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("transactionIsolation", String.valueOf(isolationLevel));
    }

    @Override
    public Connection getConnection(JdbcSplit split)
            throws SQLException
    {
        Connection connection = driver.connect(split.getConnectionUrl(), toProperties(split.getConnectionProperties()));
        try {
            connection.setReadOnly(true);
            connection.setTransactionIsolation(isolationLevel);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public Connection getConnection(JdbcOutputTableHandle handle)
            throws SQLException
    {
        Connection connection = driver.connect(connectionUrl, connectionProperties);
        try {
            connection.setTransactionIsolation(isolationLevel);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    protected String toSqlType(Type type)
    {
        String sqlType = super.toSqlType(type);
        switch (sqlType) {
            case "time with timezone":
                return "time";
            case "timestamp":
            case "timestamp with timezone":
                return "datetime2";
            case "double precision":
                return "float";
        }
        return sqlType;
    }

    // todo: PR to make toProperties protected method in BaseJdbcClient.java
    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }
}
