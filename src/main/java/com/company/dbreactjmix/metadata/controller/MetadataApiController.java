package com.company.dbreactjmix.metadata.controller;

import com.company.dbreactjmix.metadata.db.service.MetadataJdbcService;
import com.company.dbreactjmix.metadata.db.service.ConnectionConfigService;
import com.company.dbreactjmix.metadata.db.service.MetaSetSnapshotService;
import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.QueryBuildRequest;
import com.company.dbreactjmix.metadata.dto.RawQueryRequest;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.query.SqlBuilderService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

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
        try {
            String sql = sqlBuilderService.buildSelectSql(request);
            return Map.of("sql", sql);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @PostMapping("/query/execute")
    public Map<String, Object> executeBuiltQuery(@RequestBody QueryBuildRequest request) {
        try {
            String sql = sqlBuilderService.buildSelectSql(request);
            List<Map<String, Object>> data = metadataJdbcService.runSelectQuery(request.getConnection(), sql);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("sql", sql);
            response.put("count", data.size());
            response.put("data", data);
            return response;
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @PostMapping("/query/raw/execute")
    public Map<String, Object> executeRawQuery(@RequestBody RawQueryRequest request) {
        try {
            List<Map<String, Object>> data = metadataJdbcService.runSelectQuery(request.getConnection(), request.getSql());

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("sql", request.getSql());
            response.put("count", data.size());
            response.put("data", data);
            return response;
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @PostMapping("/metapack")
    public MetaPackDto buildMetaPack(@RequestBody DbConnectionRequest request) {
        try {
            return metadataJdbcService.buildMetaPack(request);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @PostMapping("/metapack/save")
    public Map<String, Object> saveMetaPack(@RequestBody SaveMetaPackRequest request) {
        try {
            return metaSetSnapshotService.saveSnapshot(request);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @GetMapping("/metapack/versions")
    public List<Map<String, Object>> listMetaPackVersions(@RequestParam("metaSetCode") String metaSetCode) {
        try {
            return metaSetSnapshotService.listVersions(metaSetCode);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @GetMapping("/metapack/version")
    public Map<String, Object> getMetaPackVersion(
            @RequestParam("metaSetCode") String metaSetCode,
            @RequestParam("versionNo") Integer versionNo
    ) {
        try {
            return metaSetSnapshotService.getVersion(metaSetCode, versionNo);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @PostMapping("/connection")
    public Map<String, Object> saveConnection(@RequestBody DbConnectionRequest request) {
        try {
            return connectionConfigService.toResponse(connectionConfigService.save(request));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }
}
