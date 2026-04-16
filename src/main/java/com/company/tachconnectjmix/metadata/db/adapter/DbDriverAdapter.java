package com.company.tachconnectjmix.metadata.db.adapter;

import com.company.tachconnectjmix.metadata.dto.DbConnectionRequest;
import com.company.tachconnectjmix.metadata.enums.DatabaseType;

public interface DbDriverAdapter {

    DatabaseType getSupportedDbType();

    String buildJdbcUrl(DbConnectionRequest request);

    String getDriverClassName();
}
