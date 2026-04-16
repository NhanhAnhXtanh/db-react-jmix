package com.company.tachconnectjmix.metadata.dto;

public class RawQueryRequest {

    private DbConnectionRequest connection;
    private String sql;

    public DbConnectionRequest getConnection() {
        return connection;
    }

    public void setConnection(DbConnectionRequest connection) {
        this.connection = connection;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
