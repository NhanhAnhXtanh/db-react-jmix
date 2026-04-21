package com.company.dbreactjmix.metadata.dto;

public class SyncConfirmRequest {

    private String metaSetCode;
    private String metaSetName;
    private String connectionCode;
    private DbConnectionRequest connection;

    public String getMetaSetCode() {
        return metaSetCode;
    }

    public void setMetaSetCode(String metaSetCode) {
        this.metaSetCode = metaSetCode;
    }

    public String getMetaSetName() {
        return metaSetName;
    }

    public void setMetaSetName(String metaSetName) {
        this.metaSetName = metaSetName;
    }

    public String getConnectionCode() {
        return connectionCode;
    }

    public void setConnectionCode(String connectionCode) {
        this.connectionCode = connectionCode;
    }

    public DbConnectionRequest getConnection() {
        return connection;
    }

    public void setConnection(DbConnectionRequest connection) {
        this.connection = connection;
    }
}
