package com.company.dbreactjmix.metadata.dto;

public class MetaSyncPollRequest {

    private String packCode;
    private DbConnectionRequest connection;

    public String getPackCode() {
        return packCode;
    }

    public void setPackCode(String packCode) {
        this.packCode = packCode;
    }

    public DbConnectionRequest getConnection() {
        return connection;
    }

    public void setConnection(DbConnectionRequest connection) {
        this.connection = connection;
    }
}
