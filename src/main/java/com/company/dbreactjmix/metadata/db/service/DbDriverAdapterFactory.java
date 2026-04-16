package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.db.adapter.DbDriverAdapter;
import com.company.dbreactjmix.metadata.enums.DatabaseType;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class DbDriverAdapterFactory {

    private final Map<DatabaseType, DbDriverAdapter> adapterMap;

    public DbDriverAdapterFactory(List<DbDriverAdapter> adapters) {
        this.adapterMap = adapters.stream()
                .collect(Collectors.toMap(DbDriverAdapter::getSupportedDbType, Function.identity()));
    }

    public DbDriverAdapter getAdapter(DatabaseType type) {
        return Optional.ofNullable(adapterMap.get(type))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported DB type: " + type));
    }
}
