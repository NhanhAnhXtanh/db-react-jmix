package com.company.dbreactjmix.metadata.controller;

import com.company.dbreactjmix.metadata.db.service.MetadataJdbcService;
import com.company.dbreactjmix.metadata.db.service.ApiMetadataService;
import com.company.dbreactjmix.metadata.db.service.ConnectionConfigService;
import com.company.dbreactjmix.metadata.db.service.DbConnectionService;
import com.company.dbreactjmix.metadata.db.service.MetaSyncCommitService;
import com.company.dbreactjmix.metadata.db.service.MetaSyncService;
import com.company.dbreactjmix.metadata.db.service.MongoMetadataService;
import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaSyncCommitRequest;
import com.company.dbreactjmix.metadata.dto.QueryBuildRequest;
import com.company.dbreactjmix.metadata.dto.RawQueryRequest;
import com.company.dbreactjmix.metadata.dto.SchemaSnapshotDto;
import com.company.dbreactjmix.metadata.dto.SchemaDiffDto;
import com.company.dbreactjmix.metadata.dto.SyncCheckRequest;
import com.company.dbreactjmix.metadata.dto.SyncConfirmRequest;
import com.company.dbreactjmix.metadata.enums.DatabaseType;
import com.company.dbreactjmix.metadata.query.SqlBuilderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/metadata")
public class MetadataApiController {

    private final MetadataJdbcService metadataJdbcService;
    private final SqlBuilderService sqlBuilderService;
    private final ConnectionConfigService connectionConfigService;
    private final DbConnectionService dbConnectionService;
    private final MetaSyncService metaSyncService;
    private final MongoMetadataService mongoMetadataService;
    private final ApiMetadataService apiMetadataService;
    private final MetaSyncCommitService metaSyncCommitService;

    public MetadataApiController(
            MetadataJdbcService metadataJdbcService,
            SqlBuilderService sqlBuilderService,
            ConnectionConfigService connectionConfigService,
            DbConnectionService dbConnectionService,
            MetaSyncService metaSyncService,
            MongoMetadataService mongoMetadataService,
            ApiMetadataService apiMetadataService,
            MetaSyncCommitService metaSyncCommitService
    ) {
        this.metadataJdbcService = metadataJdbcService;
        this.sqlBuilderService = sqlBuilderService;
        this.connectionConfigService = connectionConfigService;
        this.dbConnectionService = dbConnectionService;
        this.metaSyncService = metaSyncService;
        this.mongoMetadataService = mongoMetadataService;
        this.apiMetadataService = apiMetadataService;
        this.metaSyncCommitService = metaSyncCommitService;
    }

    @GetMapping("/ping")
    public Map<String, Object> ping() {
        return Map.of("status", "ok");
    }

    @PostMapping("/query/preview")
    public Map<String, Object> previewQuery(@RequestBody QueryBuildRequest request) {
        return Map.of("sql", sqlBuilderService.buildSelectSql(request));
    }

    @PostMapping("/query/execute")
    public Map<String, Object> executeBuiltQuery(@RequestBody QueryBuildRequest request) {
        String sql = sqlBuilderService.buildSelectSql(request);
        List<Map<String, Object>> data = metadataJdbcService.runSelectQuery(request.getConnection(), sql);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("sql", sql);
        response.put("count", data.size());
        response.put("data", data);
        return response;
    }

    @PostMapping("/query/raw/execute")
    public Map<String, Object> executeRawQuery(@RequestBody RawQueryRequest request) {
        if (request.getConnection().getDatabaseType() == DatabaseType.RESTAPI) {
            throw new IllegalArgumentException("Use REST API tab to execute API requests");
        }

        List<Map<String, Object>> data = request.getConnection().getDatabaseType() == DatabaseType.MONGODB
                ? mongoMetadataService.runReadQuery(request.getConnection(), request.getSql())
                : metadataJdbcService.runSelectQuery(request.getConnection(), request.getSql());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("sql", request.getSql());
        response.put("count", data.size());
        response.put("data", data);
        return response;
    }

    @PostMapping("/schema")
    public SchemaSnapshotDto buildSchema(@RequestBody DbConnectionRequest request) {
        if (request.getDatabaseType() == DatabaseType.MONGODB) {
            return mongoMetadataService.buildSchema(request);
        }
        if (request.getDatabaseType() == DatabaseType.RESTAPI) {
            return apiMetadataService.buildSchema(request);
        }
        return metadataJdbcService.buildSchema(request);
    }

    @PostMapping("/mongodb/schema/deep-scan")
    public Map<String, Object> startMongoDeepScan(@RequestBody DbConnectionRequest request) {
        if (request.getDatabaseType() != DatabaseType.MONGODB) {
            throw new IllegalArgumentException("Deep scan is only available for MongoDB connections");
        }
        return mongoMetadataService.startDeepScan(request);
    }

    @PostMapping("/mongodb/schema/status")
    public Map<String, Object> getMongoScanStatus(@RequestBody DbConnectionRequest request) {
        if (request.getDatabaseType() != DatabaseType.MONGODB) {
            throw new IllegalArgumentException("Mongo scan status is only available for MongoDB connections");
        }
        return mongoMetadataService.getScanStatus(request);
    }

    @GetMapping("/sync/latest")
    public List<Map<String, Object>> getLatestMetaSync(@RequestParam("packCode") String packCode) {
        List<Map<String, Object>> data = metaSyncService.getLatestMetaSyncSchema(packCode);
        return data != null ? data : List.of();
    }

    @PostMapping("/sync/preview")
    public SchemaDiffDto previewSync(@RequestBody SyncCheckRequest request) {
        return metaSyncService.previewSync(request);
    }

    @PostMapping("/sync/accept")
    public Map<String, Object> acceptSync(@RequestBody SyncConfirmRequest request) {
        return metaSyncService.acceptSync(request);
    }

    @PostMapping("/metasync/commit")
    public Map<String, Object> commitMetaSync(@RequestBody MetaSyncCommitRequest request) {
        return metaSyncCommitService.commit(request);
    }

    @GetMapping("/metasync/{packCode}/commits")
    public List<Map<String, Object>> listMetaSyncCommits(@PathVariable("packCode") String packCode) {
        return metaSyncCommitService.listCommits(packCode);
    }

    @GetMapping("/metasync/{packCode}/commit/{versionNo}")
    public Map<String, Object> getMetaSyncCommit(
            @PathVariable("packCode") String packCode,
            @PathVariable("versionNo") Integer versionNo
    ) {
        Map<String, Object> detail = metaSyncCommitService.getCommitDetail(packCode, versionNo);
        return detail != null ? detail : Map.of();
    }

    @GetMapping("/connections")
    public List<Map<String, Object>> listConnections() {
        return connectionConfigService.listAll()
                .stream()
                .map(connectionConfigService::toResponse)
                .collect(java.util.stream.Collectors.toList());
    }

    @PostMapping("/connection")
    public Map<String, Object> saveConnection(@RequestBody DbConnectionRequest request) {
        return connectionConfigService.toResponse(connectionConfigService.save(request));
    }

    @PostMapping("/connection/test")
    public Map<String, Object> testConnection(@RequestBody DbConnectionRequest request) throws Exception {
        if (request.getDatabaseType() == DatabaseType.MONGODB) {
            return mongoMetadataService.testConnection(request);
        }
        if (request.getDatabaseType() == DatabaseType.POSTGRES) {
            try (java.sql.Connection connection = dbConnectionService.getConnection(request)) {
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("status", "ok");
                response.put("database", connection.getCatalog());
                response.put("product", connection.getMetaData().getDatabaseProductName());
                return response;
            }
        }
        if (request.getDatabaseType() == DatabaseType.RESTAPI) {
            SchemaSnapshotDto schemaSnapshot = apiMetadataService.buildSchema(request);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "ok");
            response.put("database", request.getDbName());
            response.put("product", "REST API");
            response.put("endpoints", schemaSnapshot.getSchema().stream()
                    .filter(item -> item.getPath_parent() == null)
                    .count());
            return response;
        }
        throw new IllegalArgumentException("Connection test is not supported for this database type");
    }
}
