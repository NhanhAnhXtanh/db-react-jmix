package com.company.dbreactjmix.metadata.dto;

public class SyncCheckRequest {

    private String metaSetCode;
    private DbConnectionRequest connection;

    public String getMetaSetCode() {
        return metaSetCode;
    }

    public void setMetaSetCode(String metaSetCode) {
        this.metaSetCode = metaSetCode;
    }

    public DbConnectionRequest getConnection() {
        return connection;
    }

    public void setConnection(DbConnectionRequest connection) {
        this.connection = connection;
    }
}
