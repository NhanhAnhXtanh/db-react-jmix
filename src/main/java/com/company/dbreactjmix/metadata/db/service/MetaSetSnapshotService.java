package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.dto.SchemaDiffDto;
import com.company.dbreactjmix.metadata.dto.SyncCheckRequest;
import com.company.dbreactjmix.metadata.dto.SyncConfirmRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MetaSetSnapshotService {

    private final MetaSnapshotPersistenceService snapshotPersistenceService;
    private final MetaSyncService metaSyncService;

    public MetaSetSnapshotService(
            MetaSnapshotPersistenceService snapshotPersistenceService,
            MetaSyncService metaSyncService
    ) {
        this.snapshotPersistenceService = snapshotPersistenceService;
        this.metaSyncService = metaSyncService;
    }

    public Map<String, Object> saveSnapshot(SaveMetaPackRequest request) {
        validateSaveRequest(request);
        return snapshotPersistenceService.saveSnapshot(request);
    }

    public List<Map<String, Object>> listVersions(String metaSetCode) {
        if (metaSetCode == null || metaSetCode.isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        return snapshotPersistenceService.listVersions(metaSetCode);
    }

    public Map<String, Object> getVersion(String metaSetCode, Integer versionNo) {
        if (metaSetCode == null || metaSetCode.isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        if (versionNo == null) {
            throw new IllegalArgumentException("versionNo is required");
        }
        return snapshotPersistenceService.getVersion(metaSetCode, versionNo);
    }

    public List<Map<String, Object>> listMetaPacks() {
        return snapshotPersistenceService.listMetaPacks();
    }

    public List<Map<String, Object>> listPackVersions(String packCode) {
        if (packCode == null || packCode.isBlank()) {
            throw new IllegalArgumentException("packCode is required");
        }
        return snapshotPersistenceService.listPackVersions(packCode);
    }

    public MetaPackDto getLatestPackSchema(String packCode) {
        if (packCode == null || packCode.isBlank()) {
            return null;
        }
        return snapshotPersistenceService.getLatestPackSchema(packCode);
    }

    public List<Map<String, Object>> getLatestMetaSyncSchema(String packCode) {
        if (packCode == null || packCode.isBlank()) {
            return null;
        }
        return metaSyncService.getLatestMetaSyncSchema(packCode);
    }

    public SchemaDiffDto previewSync(SyncCheckRequest request) {
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        return metaSyncService.previewSync(request);
    }

    public Map<String, Object> acceptSync(SyncConfirmRequest request) {
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        return metaSyncService.acceptSync(request);
    }

    private void validateSaveRequest(SaveMetaPackRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request is required");
        }
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        if (request.getMetaPack() == null) {
            throw new IllegalArgumentException("metaPack is required");
        }
    }
}
