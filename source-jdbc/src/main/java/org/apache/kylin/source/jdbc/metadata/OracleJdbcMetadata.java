package org.apache.kylin.source.jdbc.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.source.hive.DBConnConf;
import org.apache.kylin.source.jdbc.SqlUtil;

/*
date:2019/07/24
author:jwp
*/
public class OracleJdbcMetadata extends DefaultJdbcMetadata {

    public OracleJdbcMetadata(DBConnConf dbConnConf) {
        super(dbConnConf);
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = SqlUtil.getConnection(dbconf); ResultSet rs = con.getMetaData().getSchemas()) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                ret.add(schema);
            }
        }
        return ret;
    }

    @Override
    public List<String> listTables(String schema) throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = SqlUtil.getConnection(dbconf);
                ResultSet rs = con.getMetaData().getTables(null, schema, null, null)) {
            while (rs.next()) {
                String name = rs.getString("TABLE_NAME");
                ret.add(name);
            }
        }
        return ret;
    }

    @Override
    public ResultSet getTable(final DatabaseMetaData dbmd, String schema, String table) throws SQLException {
        return dbmd.getTables(null, schema, table, null);
    }

    @Override
    public ResultSet listColumns(final DatabaseMetaData dbmd, String schema, String table) throws SQLException {
        return dbmd.getColumns(null, schema, table, null);
    }

}
