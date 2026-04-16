package com.company.dbreactjmix.metadata.db.adapter;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.enums.DatabaseType;

public interface DbDriverAdapter {

    DatabaseType getSupportedDbType();

    String buildJdbcUrl(DbConnectionRequest request);

    String getDriverClassName();
}
