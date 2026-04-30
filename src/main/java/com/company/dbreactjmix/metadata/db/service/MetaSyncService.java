package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.SchemaSnapshotDto;
import com.company.dbreactjmix.metadata.dto.SchemaDiffDto;
import com.company.dbreactjmix.metadata.dto.SyncCheckRequest;
import com.company.dbreactjmix.metadata.dto.SyncConfirmRequest;
import com.company.dbreactjmix.metadata.entity.MetadataConnectionConfig;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSync;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.jmix.core.DataManager;
import io.jmix.core.security.SystemAuthenticator;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class MetaSyncService {

    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;
    private final MetadataJdbcService metadataJdbcService;
    private final MetaSnapshotCodec codec;
    private final MetaSchemaDiffService diffService;

    public MetaSyncService(
            DataManager dataManager,
            SystemAuthenticator systemAuthenticator,
            MetadataJdbcService metadataJdbcService,
            MetaSnapshotCodec codec,
            MetaSchemaDiffService diffService
    ) {
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
        this.metadataJdbcService = metadataJdbcService;
        this.codec = codec;
        this.diffService = diffService;
    }

    public SchemaDiffDto previewSync(SyncCheckRequest request) {
        SchemaSnapshotDto currentPack = metadataJdbcService.readDatabaseSchema(request.getConnection());
        List<MetaSetModelDto> allSchema = currentPack.getSchema() != null ? currentPack.getSchema() : List.of();

        List<MetaSetModelDto> currentTables = allSchema.stream()
                .filter(field -> field.getPath_parent() == null)
                .collect(Collectors.toList());
        Map<String, List<MetaSetModelDto>> currentColumnsByTable = allSchema.stream()
                .filter(field -> field.getPath_parent() != null)
                .collect(Collectors.groupingBy(MetaSetModelDto::getPath_parent));

        List<Map<String, Object>> diffs = systemAuthenticator.withSystem(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            Map<String, List<MetaSetModelDto>> previousColumnsByTable = loadLatestMetaSyncColumnsByPackCode(request.getMetaSetCode());

            java.util.Set<String> currentTableCodes = currentTables.stream()
                    .map(MetaSetModelDto::getCode)
                    .collect(Collectors.toSet());

            boolean allHashesMatch = !previousColumnsByTable.isEmpty()
                    && currentTableCodes.size() == previousColumnsByTable.keySet().size();
            if (allHashesMatch) {
                for (MetaSetModelDto table : currentTables) {
                    List<MetaSetModelDto> currentColumns = currentColumnsByTable.getOrDefault(table.getCode(), List.of());
                    List<MetaSetModelDto> previousColumns = previousColumnsByTable.get(table.getCode());
                    if (previousColumns == null || !codec.toColumnsHash(currentColumns).equals(codec.toColumnsHash(previousColumns))) {
                        allHashesMatch = false;
                        break;
                    }
                }
            }

            if (allHashesMatch) {
                return new ArrayList<>();
            }

            for (MetaSetModelDto table : currentTables) {
                List<MetaSetModelDto> currentColumns = currentColumnsByTable.getOrDefault(table.getCode(), List.of());
                if (!previousColumnsByTable.containsKey(table.getCode())) {
                    Map<String, Object> diff = new LinkedHashMap<>();
                    diff.put("tableCode", table.getCode());
                    diff.put("isNewTable", true);
                    diff.put("added", currentColumns.stream().map(MetaSetModelDto::getCode).sorted().collect(Collectors.toList()));
                    diff.put("removed", List.of());
                    diff.put("changed", List.of());
                    result.add(diff);
                    continue;
                }

                List<MetaSetModelDto> previousColumns = previousColumnsByTable.get(table.getCode());
                if (!codec.toColumnsHash(currentColumns).equals(codec.toColumnsHash(previousColumns))) {
                    result.add(diffService.computeDiff(table.getCode(), previousColumns, currentColumns));
                }
            }

            for (Map.Entry<String, List<MetaSetModelDto>> entry : previousColumnsByTable.entrySet()) {
                if (!currentTableCodes.contains(entry.getKey())) {
                    Map<String, Object> diff = new LinkedHashMap<>();
                    diff.put("tableCode", entry.getKey());
                    diff.put("isDeletedTable", true);
                    diff.put("added", List.of());
                    diff.put("removed", entry.getValue().stream().map(MetaSetModelDto::getCode).sorted().collect(Collectors.toList()));
                    diff.put("changed", List.of());
                    result.add(diff);
                }
            }

            return result;
        });

        return diffService.transformToNestedShape(diffs);
    }

    public Map<String, Object> acceptSync(SyncConfirmRequest request) {
        SchemaSnapshotDto schemaSnapshot = metadataJdbcService.readDatabaseSchema(request.getConnection());
        List<MetaSetModelDto> allSchema = schemaSnapshot.getSchema() != null ? schemaSnapshot.getSchema() : List.of();
        List<MetaSetModelDto> tables = allSchema.stream()
                .filter(field -> field.getPath_parent() == null)
                .collect(Collectors.toList());
        Map<String, List<MetaSetModelDto>> columnsByTable = allSchema.stream()
                .filter(field -> field.getPath_parent() != null)
                .collect(Collectors.groupingBy(MetaSetModelDto::getPath_parent));

        MetadataConnectionConfig connectionConfig = null;
        if (request.getConnectionCode() != null && !request.getConnectionCode().isBlank()) {
            connectionConfig = systemAuthenticator.withSystem(() ->
                    dataManager.load(MetadataConnectionConfig.class)
                            .query("e.code = :code")
                            .parameter("code", request.getConnectionCode())
                            .optional()
                            .orElse(null)
            );
        }

        int changedCount = upsertMetaSync(request.getMetaSetCode(), request.getMetaSetName(), tables, columnsByTable, connectionConfig);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("changed", changedCount > 0);
        result.put("savedCount", changedCount);
        return result;
    }

    public List<Map<String, Object>> getLatestMetaSyncSchema(String packCode) {
        return systemAuthenticator.withSystem(() -> {
            List<Map<String, Object>> result = toMetaSyncSchemaResponse(loadLatestMetaSyncRowsByPackCode(packCode));
            return result.isEmpty() ? null : result;
        });
    }

    private int upsertMetaSync(
            String packCode,
            String packName,
            List<MetaSetModelDto> tables,
            Map<String, List<MetaSetModelDto>> columnsByTable,
            MetadataConnectionConfig connectionConfig
    ) {
        return systemAuthenticator.withSystem(() -> {
            Map<String, String> existingHashes = loadLatestMetaSyncHashesByPackCode(packCode);

            Map<String, String> currentHashes = new LinkedHashMap<>();
            for (MetaSetModelDto table : tables) {
                String tableCode = table.getCode();
                List<MetaSetModelDto> columns = columnsByTable.getOrDefault(tableCode, List.of());
                currentHashes.put(tableCode, codec.toColumnsHash(columns));
            }

            boolean anyChange = !currentHashes.keySet().equals(existingHashes.keySet());
            if (!anyChange) {
                for (Map.Entry<String, String> entry : currentHashes.entrySet()) {
                    if (!Objects.equals(entry.getValue(), existingHashes.get(entry.getKey()))) {
                        anyChange = true;
                        break;
                    }
                }
            }

            if (!anyChange) {
                return 0;
            }

            int writtenCount = 0;
            for (MetaSetModelDto table : tables) {
                String tableCode = table.getCode();
                String hash = currentHashes.get(tableCode);
                String previousHash = existingHashes.get(tableCode);

                if (Objects.equals(hash, previousHash)) {
                    continue;
                }

                String metaSetCode = packCode + "-" + tableCode;
                List<MetaSetModelDto> columns = columnsByTable.getOrDefault(tableCode, List.of());

                MetaSet metaSet = findOrCreateMetaSet(metaSetCode,
                        packName + "." + (table.getName() != null ? table.getName() : tableCode));
                String fieldData = codec.toTableMetaSyncFieldData(tableCode, columns);

                int nextVer = findMaxSyncVersionForTable(metaSetCode) + 1;

                MetaSync sync = dataManager.create(MetaSync.class);
                sync.setMetaSet(metaSet);
                sync.setConnectionConfig(connectionConfig);
                sync.setFieldData(fieldData);
                sync.setHashData(hash);
                sync.setSyncVersionNo(nextVer);
                sync.setIsDeleted(false);
                dataManager.save(sync);
                writtenCount++;
            }

            for (Map.Entry<String, String> entry : existingHashes.entrySet()) {
                String previousTableCode = entry.getKey();
                if (currentHashes.containsKey(previousTableCode)) {
                    continue;
                }
                String metaSetCode = packCode + "-" + previousTableCode;
                MetaSet metaSet = findOrCreateMetaSet(metaSetCode,
                        packName + "." + previousTableCode);

                int nextVer = findMaxSyncVersionForTable(metaSetCode) + 1;

                MetaSync sync = dataManager.create(MetaSync.class);
                sync.setMetaSet(metaSet);
                sync.setConnectionConfig(connectionConfig);
                sync.setFieldData(codec.toTableMetaSyncFieldData(previousTableCode, List.of()));
                sync.setHashData(entry.getValue());
                sync.setSyncVersionNo(nextVer);
                sync.setIsDeleted(true);
                dataManager.save(sync);
                writtenCount++;
            }

            return writtenCount;
        });
    }

    private int findMaxSyncVersionForTable(String metaSetCode) {
        Integer max = dataManager.loadValue(
                "select max(e.syncVersionNo) from MetaSync e where e.metaSet.code = :code",
                Integer.class
        ).parameter("code", metaSetCode).optional().orElse(0);
        return max != null ? max : 0;
    }

    private Map<String, List<MetaSetModelDto>> loadLatestMetaSyncColumnsByPackCode(String packCode) {
        List<MetaSync> latestRows = loadLatestMetaSyncRowsByPackCode(packCode);
        Map<String, List<MetaSetModelDto>> result = new LinkedHashMap<>();
        for (MetaSync sync : latestRows) {
            if (sync.getFieldData() == null) {
                continue;
            }
            try {
                JsonNode root = codec.readStorageTree(sync.getFieldData());
                String tableCode = root.has("table") ? root.get("table").asText() : null;
                if (tableCode == null || !root.has("metaset") || !root.get("metaset").isArray()) {
                    continue;
                }
                List<MetaSetModelDto> columns = new ArrayList<>();
                for (JsonNode columnNode : root.get("metaset")) {
                    columns.add(codec.storageTreeToValue(columnNode, MetaSetModelDto.class));
                }
                result.put(tableCode, columns);
            } catch (Exception ignored) {
            }
        }
        return result;
    }

    private Map<String, String> loadLatestMetaSyncHashesByPackCode(String packCode) {
        Map<String, String> result = new LinkedHashMap<>();
        for (MetaSync sync : loadLatestMetaSyncRowsByPackCode(packCode)) {
            if (sync.getFieldData() == null) {
                continue;
            }
            try {
                JsonNode root = codec.readStorageTree(sync.getFieldData());
                String tableCode = root.has("table") ? root.get("table").asText() : null;
                if (tableCode != null) {
                    result.put(tableCode, sync.getHashData());
                }
            } catch (Exception ignored) {
            }
        }
        return result;
    }

    private List<MetaSync> loadLatestMetaSyncRowsByPackCode(String packCode) {
        String codePrefix = packCode + "-%";
        List<MetaSync> rows = dataManager.load(MetaSync.class)
                .query("select e from MetaSync e " +
                        "where e.metaSet.code like :codePrefix " +
                        "and e.syncVersionNo = (select max(x.syncVersionNo) from MetaSync x where x.metaSet.id = e.metaSet.id) " +
                        "order by e.metaSet.code asc")
                .parameter("codePrefix", codePrefix)
                .list();
        List<MetaSync> result = new ArrayList<>();
        for (MetaSync sync : rows) {
            if (Boolean.TRUE.equals(sync.getIsDeleted())) {
                continue;
            }
            result.add(sync);
        }
        return result;
    }

    private List<Map<String, Object>> toMetaSyncSchemaResponse(List<MetaSync> rows) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (MetaSync sync : rows) {
            if (sync.getFieldData() == null) {
                continue;
            }
            try {
                JsonNode root = codec.readStorageTree(sync.getFieldData());
                result.add(codec.storageTreeToMap(root));
            } catch (Exception ignored) {
            }
        }
        return result;
    }

    private MetaSet findOrCreateMetaSet(String code, String name) {
        MetaSet existing = dataManager.load(MetaSet.class)
                .query("e.code = :code")
                .parameter("code", code)
                .optional()
                .orElse(null);

        MetaSet metaSet = existing != null ? existing : dataManager.create(MetaSet.class);
        metaSet.setCode(code);
        metaSet.setName(name != null ? name : code);
        return dataManager.save(metaSet);
    }
}
