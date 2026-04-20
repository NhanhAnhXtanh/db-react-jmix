package com.company.dbreactjmix.metadata.dto;

public class SyncConfirmRequest {

    private String metaSetCode;
    private String metaSetName;
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

    public DbConnectionRequest getConnection() {
        return connection;
    }

    public void setConnection(DbConnectionRequest connection) {
        this.connection = connection;
    }
}
