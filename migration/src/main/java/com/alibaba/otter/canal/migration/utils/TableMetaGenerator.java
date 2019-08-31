package com.alibaba.otter.canal.migration.utils;

import com.alibaba.otter.canal.migration.metadata.ColumnMeta;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bucketli 2019/8/12 7:50 PM
 * @since 1.1.3
 **/
public class TableMetaGenerator {

    public static List<MigrationTable> getTableMetaWithoutColumn(final DataSource dataSource, final String schemaName,
                                                                 final String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return (List<MigrationTable>) jdbcTemplate.execute(new ConnectionCallback() {

            @Override
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = connection.getMetaData();
                List<MigrationTable> re = Lists.newArrayList();
                String sName = getIdentifierName(schemaName, metaData);
                String tName = getIdentifierName(tableName, metaData);
                ResultSet rs = metaData.getTables(sName, sName, tName, new String[] { "TABLE" });
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    String type = rs.getString(4);
                    MigrationTable t = new MigrationTable(type, StringUtils.isEmpty(catlog) ? schema : catlog, name);
                    re.add(t);
                }

                rs.close();
                return re;
            }
        });
    }

    public static void buildColumns(DataSource dataSource, final MigrationTable table) {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        template.execute(new ConnectionCallback() {

            @Override
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = connection.getMetaData();
                List<ColumnMeta> columnList = new ArrayList<>();
                ResultSet rs = metaData.getPrimaryKeys(table.getSchema(), table.getSchema(), table.getName());
                List<String> primaryKeys = new ArrayList<>();
                while (rs.next()) {
                    String name = rs.getString(3);
                    primaryKeys.add(name);
                }
                rs.close();

                rs = metaData.getColumns(table.getSchema(), table.getSchema(), table.getName(), null);
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    if ((table.getSchema() == null || LikeUtil.isMatch(table.getSchema(), catlog)
                         || LikeUtil.isMatch(table.getSchema(), schema))
                        && LikeUtil.isMatch(table.getName(), name)) {
                        String cName = rs.getString(4);
                        int cType = rs.getInt(5);
                        String typeName = rs.getString(6);
                        cType = convertSQLType(cType, typeName);
                        ColumnMeta col = new ColumnMeta(cName, cType, primaryKeys.contains(cName));
                        columnList.add(col);
                    }
                }

                rs.close();
                table.setColumns(columnList);
                return null;
            }
        });
    }

    private static int convertSQLType(int columnType, String typeName) {
        String[] typeSplit = typeName.split(" ");

        if (typeSplit.length > 1 && columnType == Types.INTEGER
            && StringUtils.equalsIgnoreCase(typeSplit[1], "UNSIGNED")) {
            columnType = Types.BIGINT;
        }

        return columnType;
    }

    private static String getIdentifierName(String name, DatabaseMetaData metaData) throws SQLException {
        if (metaData.storesMixedCaseIdentifiers()) {
            return name;
        } else if (metaData.storesUpperCaseIdentifiers()) {
            return StringUtils.upperCase(name);
        } else if (metaData.storesLowerCaseIdentifiers()) {
            return StringUtils.lowerCase(name);
        } else {
            return name;
        }
    }
}
