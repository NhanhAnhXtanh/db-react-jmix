package com.company.dbreactjmix.metadata.controller;

import com.company.dbreactjmix.metadata.db.service.MetadataJdbcService;
import com.company.dbreactjmix.metadata.db.service.ConnectionConfigService;
import com.company.dbreactjmix.metadata.db.service.MetaSetSnapshotService;
import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.QueryBuildRequest;
import com.company.dbreactjmix.metadata.dto.RawQueryRequest;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.dto.SchemaDiffDto;
import com.company.dbreactjmix.metadata.dto.SyncCheckRequest;
import com.company.dbreactjmix.metadata.dto.SyncConfirmRequest;
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
    private final MetaSetSnapshotService metaSetSnapshotService;

    public MetadataApiController(
            MetadataJdbcService metadataJdbcService,
            SqlBuilderService sqlBuilderService,
            ConnectionConfigService connectionConfigService,
            MetaSetSnapshotService metaSetSnapshotService
    ) {
        this.metadataJdbcService = metadataJdbcService;
        this.sqlBuilderService = sqlBuilderService;
        this.connectionConfigService = connectionConfigService;
        this.metaSetSnapshotService = metaSetSnapshotService;
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
        List<Map<String, Object>> data = metadataJdbcService.runSelectQuery(request.getConnection(), request.getSql());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("sql", request.getSql());
        response.put("count", data.size());
        response.put("data", data);
        return response;
    }

    @PostMapping("/metapack")
    public MetaPackDto buildMetaPack(@RequestBody DbConnectionRequest request) {
        return metadataJdbcService.readDatabaseSchema(request);
    }

    @PostMapping("/metapack/save")
    public Map<String, Object> saveMetaPack(@RequestBody SaveMetaPackRequest request) {
        return metaSetSnapshotService.saveSnapshot(request);
    }

    @GetMapping("/metapack/versions")
    public List<Map<String, Object>> listMetaPackVersions(@RequestParam("metaSetCode") String metaSetCode) {
        return metaSetSnapshotService.listVersions(metaSetCode);
    }

    @GetMapping("/metapack/version")
    public Map<String, Object> getMetaPackVersion(
            @RequestParam("metaSetCode") String metaSetCode,
            @RequestParam("versionNo") Integer versionNo
    ) {
        return metaSetSnapshotService.getVersion(metaSetCode, versionNo);
    }

    @GetMapping("/metapack/list")
    public List<Map<String, Object>> listMetaPacks() {
        return metaSetSnapshotService.listMetaPacks();
    }

    @GetMapping("/metapack/{code}/latest-schema")
    public MetaPackDto getLatestPackSchema(@PathVariable("code") String code) {
        return metaSetSnapshotService.getLatestPackSchema(code);
    }

    @GetMapping("/metapack/{code}/versions")
    public List<Map<String, Object>> listMetaPackVersionsByCode(@PathVariable("code") String code) {
        return metaSetSnapshotService.listPackVersions(code);
    }

    @GetMapping("/sync/latest")
    public List<Map<String, Object>> getLatestMetaSync(@RequestParam("packCode") String packCode) {
        List<Map<String, Object>> data = metaSetSnapshotService.getLatestMetaSyncSchema(packCode);
        return data != null ? data : List.of();
    }

    @PostMapping("/sync/preview")
    public SchemaDiffDto previewSync(@RequestBody SyncCheckRequest request) {
        return metaSetSnapshotService.previewSync(request);
    }

    @PostMapping("/sync/accept")
    public Map<String, Object> acceptSync(@RequestBody SyncConfirmRequest request) {
        return metaSetSnapshotService.acceptSync(request);
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
}
