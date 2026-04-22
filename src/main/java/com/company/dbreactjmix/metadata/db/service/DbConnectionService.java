package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.db.adapter.DbDriverAdapter;
import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.enums.DatabaseType;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Service
public class DbConnectionService {

    private final DbDriverAdapterFactory adapterFactory;

    public DbConnectionService(DbDriverAdapterFactory adapterFactory) {
        this.adapterFactory = adapterFactory;
    }

    public Connection getConnection(DbConnectionRequest request) throws SQLException {
        validateConnectionRequest(request);

        DatabaseType databaseType = request.getDatabaseType();
        DbDriverAdapter adapter = adapterFactory.getAdapter(databaseType);
        String url = adapter.buildJdbcUrl(request);

        try {
            Class.forName(adapter.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Driver not found for " + databaseType, e);
        }

        return DriverManager.getConnection(url, request.getUsername(), request.getPassword());
    }

    private void validateConnectionRequest(DbConnectionRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Connection request is required");
        }
        if (request.getDatabaseType() == null) {
            throw new IllegalArgumentException("databaseType is required and must be: POSTGRES or MONGODB");
        }
        if (isBlank(request.getHost())) {
            throw new IllegalArgumentException("host is required");
        }
        if (isBlank(request.getPort())) {
            throw new IllegalArgumentException("port is required");
        }
        if (isBlank(request.getDbName())) {
            throw new IllegalArgumentException("dbName is required");
        }
        if (request.getDatabaseType() != DatabaseType.MONGODB && isBlank(request.getUsername())) {
            throw new IllegalArgumentException("username is required");
        }
        if (request.getDatabaseType() != DatabaseType.MONGODB && request.getPassword() == null) {
            throw new IllegalArgumentException("password is required");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
