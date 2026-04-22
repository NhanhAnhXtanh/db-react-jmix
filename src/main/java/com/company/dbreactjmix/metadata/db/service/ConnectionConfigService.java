package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.entity.MetadataConnectionConfig;
import com.company.dbreactjmix.metadata.enums.DatabaseType;
import io.jmix.core.DataManager;
import io.jmix.core.security.SystemAuthenticator;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class ConnectionConfigService {

    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;

    public ConnectionConfigService(DataManager dataManager, SystemAuthenticator systemAuthenticator) {
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
    }

    public List<MetadataConnectionConfig> listAll() {
        return systemAuthenticator.withSystem(() ->
                dataManager.load(MetadataConnectionConfig.class)
                        .all()
                        .list()
        );
    }

    public MetadataConnectionConfig save(DbConnectionRequest request) {
        validateRequest(request);

        return systemAuthenticator.withSystem(() -> {
            String code = buildCode(request);

            MetadataConnectionConfig existing = findByCode(code);

            MetadataConnectionConfig config = existing != null ? existing : dataManager.create(MetadataConnectionConfig.class);
            config.setCode(code);
            config.setDatabaseType(request.getDatabaseType().name());
            config.setHost(request.getHost());
            config.setPort(request.getPort());
            config.setDbName(request.getDbName());
            config.setUsername(nullToEmpty(request.getUsername()));
            config.setPassword(nullToEmpty(request.getPassword()));
            config.setSchema(request.getSchema());

            return dataManager.save(config);
        });
    }

    public Map<String, Object> toResponse(MetadataConnectionConfig config) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", config.getId());
        response.put("code", config.getCode());
        response.put("databaseType", config.getDatabaseType());
        response.put("host", config.getHost());
        response.put("port", config.getPort());
        response.put("dbName", config.getDbName());
        response.put("username", config.getUsername());
        response.put("password", config.getPassword());
        response.put("schema", config.getSchema());
        return response;
    }

    private MetadataConnectionConfig findByCode(String code) {
        return dataManager.load(MetadataConnectionConfig.class)
                .query("e.code = :code")
                .parameter("code", code)
                .optional()
                .orElse(null);
    }

    public String buildCode(DbConnectionRequest request) {
        String host = request.getHost().replaceAll("[^A-Za-z0-9]", "-");
        return String.format("%s-%s-%s-%s",
                request.getDatabaseType().name().toLowerCase(),
                host,
                request.getPort(),
                request.getDbName());
    }

    private void validateRequest(DbConnectionRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Connection request is required");
        }
        if (request.getDatabaseType() == null) {
            throw new IllegalArgumentException("databaseType is required");
        }
        if (isBlank(request.getHost())) {
            throw new IllegalArgumentException("host is required");
        }
        if (request.getDatabaseType() != DatabaseType.RESTAPI && isBlank(request.getPort())) {
            throw new IllegalArgumentException("port is required");
        }
        if (request.getDatabaseType() != DatabaseType.RESTAPI && isBlank(request.getDbName())) {
            throw new IllegalArgumentException("dbName is required");
        }
        if (request.getDatabaseType() == DatabaseType.POSTGRES && isBlank(request.getUsername())) {
            throw new IllegalArgumentException("username is required");
        }
        if (request.getDatabaseType() == DatabaseType.POSTGRES && request.getPassword() == null) {
            throw new IllegalArgumentException("password is required");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
