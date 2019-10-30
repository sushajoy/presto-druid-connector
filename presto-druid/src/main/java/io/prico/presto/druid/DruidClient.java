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
package io.prico.presto.druid;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;
import javax.inject.Inject;
import org.apache.calcite.jdbc.Driver;
import org.codehaus.commons.compiler.CompilerFactoryFactory;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;

public class DruidClient
        extends BaseJdbcClient {

    static {
        try {
            //we need to register janino in plugin classloader, otherwise calcite will fail
            CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            //do nothing
        }
    }

    @Inject
    public DruidClient(
            JdbcConnectorId connectorId,
            BaseJdbcConfig config,
            DruidConfig druidConfig) throws SQLException {
        super(connectorId, config, "", connectionFactory(config, druidConfig));
    }

    private static ConnectionFactory connectionFactory(
            BaseJdbcConfig config,
            DruidConfig druidConfig) throws SQLException {

        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("schemaFactory", "org.apache.calcite.adapter.druid.DruidSchemaFactory");
        connectionProperties.setProperty("schema.url", druidConfig.getBrokerUrl());
        connectionProperties.setProperty("schema", druidConfig.getSchema());
        connectionProperties.setProperty("QUOTED_CASING", "UNCHANGED");
        connectionProperties.setProperty("UNQUOTED_CASING", "UNCHANGED");
        connectionProperties.setProperty("CASE_SENSITIVE", "true");
        connectionProperties.setProperty("schema.coordinatorUrl", druidConfig.getCoordinatorUrl());
        System.out.println("<<<<< Configuring the driver connection >>>>>>>>>>>>");

        System.out.println("<<<<< Connection URL "+ config.getConnectionUrl());
        System.out.println("<<<<< Broker URL "+ druidConfig.getBrokerUrl());
        System.out.println("<<<<< Co-ordinar URL "+ druidConfig.getCoordinatorUrl());
        System.out.println("<<<<< Schema "+ druidConfig.getSchema());
        System.out.println("<<<<< User"+ config.getConnectionUser());
        System.out.println("<<<<< Password "+ config.getConnectionPassword());
        System.out.println("<<<<< Completed Configuring the driver connection >>>>>>>>>>>>");
        
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                connectionProperties);
    }

    protected Type toPrestoType(
            int jdbcType,
            int columnSize,
            int decimalDigits) {

    	System.out.println("JDBCType ["+jdbcType +"] columnsize ["+columnSize+"] decimaldigits ["+decimalDigits+">>>>>>>>");
        if (jdbcType != Types.LONGNVARCHAR) {
        	switch (jdbcType) {
	            case Types.BIT:
	            case Types.BOOLEAN:
	                return BOOLEAN;
	            case Types.TINYINT:
	                return TINYINT;
	            case Types.SMALLINT:
	                return SMALLINT;
	            case Types.INTEGER:
	                return INTEGER;
	            case Types.BIGINT:
	                return BIGINT;
	            case Types.REAL:
	                return REAL;
	            case Types.FLOAT:
	            case Types.DOUBLE:
	                return DOUBLE;
	            case Types.NUMERIC:
	            case Types.DECIMAL:
	                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
	                if (precision > Decimals.MAX_PRECISION) {
	                    return null;
	                }
	                return createDecimalType(precision, max(decimalDigits, 0));
	            case Types.CHAR:
	            case Types.NCHAR:
	                return createCharType(min(columnSize, CharType.MAX_LENGTH));
	            case Types.VARCHAR:
	            	if(columnSize <0) {
	            		return createUnboundedVarcharType();
	            	}
	            case Types.NVARCHAR:
	            case Types.LONGVARCHAR:
	            case Types.LONGNVARCHAR:
	                if (columnSize > VarcharType.MAX_LENGTH) {
	                    return createUnboundedVarcharType();
	                }
	                return createVarcharType(columnSize);
	            case Types.BINARY:
	            case Types.VARBINARY:
	            case Types.LONGVARBINARY:
	                return VARBINARY;
	            case Types.DATE:
	                return DATE;
	            case Types.TIME:
	                return TIME;
	            case Types.TIMESTAMP:
	                return TIMESTAMP;
	        }
        return null;
           // return super.toPrestoType(jdbcType, columnSize, decimalDigits);
        }
        //for longvarchar
        if (columnSize > VarcharType.MAX_LENGTH) {
            return createUnboundedVarcharType();
        } else if (columnSize <= 0) {
            return createUnboundedVarcharType();
        }
        return createVarcharType(columnSize);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(
            JdbcTableHandle tableHandle) {
    	
        try (Connection connection = connectionFactory.openConnection()) {
        	try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                	 Type columnType = toPrestoType(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    // skip unsupported column types
                    String columnName = resultSet.getString("COLUMN_NAME");
                    if (columnType != null) {
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
                    } else {
                    	/*if ("__time".equals(columnName)) {
                            columns.add(new JdbcColumnHandle(connectorId, columnName, TimestampType.TIMESTAMP));
                        }*/
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static ResultSet getColumns(
            JdbcTableHandle tableHandle,
            DatabaseMetaData metadata) throws SQLException {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }
    
    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
    	 try (Connection connection = connectionFactory.openConnection()) {
        	DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();            
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
        	e.printStackTrace();
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
    	return new QueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain());
    }
}


