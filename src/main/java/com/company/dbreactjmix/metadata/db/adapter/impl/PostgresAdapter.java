package com.company.dbreactjmix.metadata.db.adapter.impl;

import com.company.dbreactjmix.metadata.db.adapter.DbDriverAdapter;
import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.enums.DatabaseType;
import org.springframework.stereotype.Component;

@Component
public class PostgresAdapter implements DbDriverAdapter {

    @Override
    public DatabaseType getSupportedDbType() {
        return DatabaseType.POSTGRES;
    }

    @Override
    public String buildJdbcUrl(DbConnectionRequest request) {
        return String.format("jdbc:postgresql://%s:%s/%s", request.getHost(), request.getPort(), request.getDbName());
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }
}
