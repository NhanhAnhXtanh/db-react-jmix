package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.MetaSyncCommitRequest;
import com.company.dbreactjmix.metadata.dto.MetaSyncPollRequest;
import com.company.dbreactjmix.metadata.entity.MetadataConnectionConfig;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSync;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSyncCommit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jmix.core.DataManager;
import io.jmix.core.security.SystemAuthenticator;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Quản lý commit cho MetaSync với hash dedup và lịch sử.
 * Chỉ insert MetaSync row mới cho bảng có hash thay đổi; bảng bị xóa được đánh dấu IS_DELETED=true.
 * Mỗi commit ghi vào META_SYNC_COMMIT kèm commit_message + diff_json + summary_json.
 */
@Service
public class MetaSyncCommitService {

    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;
    private final MetadataJdbcService metadataJdbcService;
    private final MetaSnapshotCodec codec;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public MetaSyncCommitService(
            DataManager dataManager,
            SystemAuthenticator systemAuthenticator,
            MetadataJdbcService metadataJdbcService,
            MetaSnapshotCodec codec
    ) {
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
        this.metadataJdbcService = metadataJdbcService;
        this.codec = codec;
    }

    public Map<String, Object> commit(MetaSyncCommitRequest request) {
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        if (request.getCommitMessage() == null || request.getCommitMessage().isBlank()) {
            throw new IllegalArgumentException("commitMessage is required");
        }

        MetaPackDto metaPackDto = metadataJdbcService.readDatabaseSchema(request.getConnection());
        MetaPackDto.MetaPackContent content = metaPackDto.getMetaPack();
        List<MetaSetModelDto> allSchema = content.getSchema() != null ? content.getSchema() : List.of();
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

        final MetadataConnectionConfig finalConnectionConfig = connectionConfig;

        return systemAuthenticator.withSystem(() -> doCommit(
                request.getMetaSetCode(),
                request.getMetaSetName(),
                request.getCommitMessage(),
                tables,
                columnsByTable,
                finalConnectionConfig
        ));
    }

    public List<Map<String, Object>> listPacks() {
        return systemAuthenticator.withSystem(() -> {
            List<MetaSyncCommit> all = dataManager.load(MetaSyncCommit.class)
                    .query("select e from MetaSyncCommit e order by e.packCode asc, e.versionNo desc")
                    .list();

            Map<String, MetaSyncCommit> latestByPack = new LinkedHashMap<>();
            for (MetaSyncCommit c : all) {
                latestByPack.putIfAbsent(c.getPackCode(), c);
            }

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map.Entry<String, MetaSyncCommit> entry : latestByPack.entrySet()) {
                MetaSyncCommit commit = entry.getValue();
                Long tableCount = dataManager.loadValue(
                        "select count(e) from MetaSet e where e.code like :prefix",
                        Long.class
                ).parameter("prefix", commit.getPackCode() + "-%").optional().orElse(0L);
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("packCode", commit.getPackCode());
                item.put("latestVersionNo", commit.getVersionNo());
                item.put("latestCommitMessage", commit.getCommitMessage());
                item.put("latestCommitDate", formatDate(commit.getCreatedDate()));
                item.put("tableCount", tableCount != null ? tableCount.intValue() : 0);
                result.add(item);
            }
            return result;
        });
    }

    public Map<String, Object> previewRich(MetaSyncPollRequest request) {
        if (request.getPackCode() == null || request.getPackCode().isBlank()) {
            throw new IllegalArgumentException("packCode is required");
        }
        if (request.getConnection() == null) {
            throw new IllegalArgumentException("connection is required");
        }

        MetaPackDto metaPackDto = readLiveSchema(request.getConnection());
        MetaPackDto.MetaPackContent content = metaPackDto.getMetaPack();
        List<MetaSetModelDto> allSchema = content.getSchema() != null ? content.getSchema() : List.of();

        List<MetaSetModelDto> tables = allSchema.stream()
                .filter(f -> f.getPath_parent() == null)
                .collect(Collectors.toList());
        Map<String, List<MetaSetModelDto>> columnsByTable = allSchema.stream()
                .filter(f -> f.getPath_parent() != null)
                .collect(Collectors.groupingBy(MetaSetModelDto::getPath_parent));

        return systemAuthenticator.withSystem(() -> {
            Map<String, MetaSync> latestActiveByTable = loadLatestActiveByPackCode(request.getPackCode());
            Map<String, List<MetaSetModelDto>> previousColumnsByTable = new LinkedHashMap<>();
            for (Map.Entry<String, MetaSync> entry : latestActiveByTable.entrySet()) {
                previousColumnsByTable.put(entry.getKey(), readColumnsFromMetaSync(entry.getValue()));
            }

            List<Map<String, Object>> tableDiffs = new ArrayList<>();
            boolean hasChanges = false;
            Map<String, MetaSetModelDto> currentTableMap = new LinkedHashMap<>();

            for (MetaSetModelDto table : tables) {
                currentTableMap.put(table.getCode(), table);
                List<MetaSetModelDto> currentColumns = columnsByTable.getOrDefault(table.getCode(), List.of());
                List<MetaSetModelDto> prevColumns = previousColumnsByTable.get(table.getCode());

                if (prevColumns == null) {
                    hasChanges = true;
                    Map<String, Object> diff = new LinkedHashMap<>();
                    diff.put("tableCode", table.getCode());
                    diff.put("tableName", table.getName());
                    diff.put("status", "added");
                    diff.put("metaSyncColumns", List.of());
                    diff.put("liveColumns", currentColumns.stream().map(this::columnAsMap).collect(Collectors.toList()));
                    diff.put("addedColumns", currentColumns.stream().map(this::columnAsMap).collect(Collectors.toList()));
                    diff.put("removedColumns", List.of());
                    diff.put("modifiedColumns", List.of());
                    tableDiffs.add(diff);
                } else {
                    MetaSync latestSync = latestActiveByTable.get(table.getCode());
                    String prevHash = latestSync != null ? latestSync.getHashData() : null;
                    String currHash = codec.toColumnsHash(currentColumns);
                    if (!Objects.equals(prevHash, currHash)) {
                        hasChanges = true;
                        Map<String, Object> diff = buildModifiedTableDiff(
                                table.getCode(), table.getName(), prevColumns, currentColumns);
                        diff.put("status", "modified");
                        diff.put("metaSyncColumns", prevColumns.stream().map(this::columnAsMap).collect(Collectors.toList()));
                        diff.put("liveColumns", currentColumns.stream().map(this::columnAsMap).collect(Collectors.toList()));
                        tableDiffs.add(diff);
                    }
                }
            }

            for (Map.Entry<String, List<MetaSetModelDto>> entry : previousColumnsByTable.entrySet()) {
                if (!currentTableMap.containsKey(entry.getKey())) {
                    hasChanges = true;
                    List<MetaSetModelDto> prevCols = entry.getValue();
                    Map<String, Object> diff = new LinkedHashMap<>();
                    diff.put("tableCode", entry.getKey());
                    diff.put("tableName", entry.getKey());
                    diff.put("status", "removed");
                    diff.put("metaSyncColumns", prevCols.stream().map(this::columnAsMap).collect(Collectors.toList()));
                    diff.put("liveColumns", List.of());
                    diff.put("addedColumns", List.of());
                    diff.put("removedColumns", prevCols.stream().map(this::columnAsMap).collect(Collectors.toList()));
                    diff.put("modifiedColumns", List.of());
                    tableDiffs.add(diff);
                }
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("hasChanges", hasChanges);
            result.put("tables", tableDiffs);
            return result;
        });
    }

    public List<Map<String, Object>> listCommits(String packCode) {
        if (packCode == null || packCode.isBlank()) {
            return List.of();
        }
        return systemAuthenticator.withSystem(() -> {
            List<MetaSyncCommit> rows = dataManager.load(MetaSyncCommit.class)
                    .query("select e from MetaSyncCommit e where e.packCode = :packCode order by e.versionNo desc")
                    .parameter("packCode", packCode)
                    .list();
            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaSyncCommit commit : rows) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", commit.getId() != null ? commit.getId().toString() : null);
                item.put("packCode", commit.getPackCode());
                item.put("versionNo", commit.getVersionNo());
                item.put("commitMessage", commit.getCommitMessage());
                item.put("summary", parseJsonSilent(commit.getSummaryJson()));
                item.put("createdBy", commit.getCreatedBy());
                item.put("createdDate", formatDate(commit.getCreatedDate()));
                result.add(item);
            }
            return result;
        });
    }

    public Map<String, Object> getCommitDetail(String packCode, Integer versionNo) {
        if (packCode == null || packCode.isBlank() || versionNo == null) {
            return null;
        }
        return systemAuthenticator.withSystem(() -> {
            MetaSyncCommit commit = dataManager.load(MetaSyncCommit.class)
                    .query("select e from MetaSyncCommit e where e.packCode = :packCode and e.versionNo = :versionNo")
                    .parameter("packCode", packCode)
                    .parameter("versionNo", versionNo)
                    .optional()
                    .orElse(null);
            if (commit == null) {
                return null;
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("id", commit.getId() != null ? commit.getId().toString() : null);
            result.put("packCode", commit.getPackCode());
            result.put("versionNo", commit.getVersionNo());
            result.put("commitMessage", commit.getCommitMessage());
            result.put("summary", parseJsonSilent(commit.getSummaryJson()));
            result.put("diff", parseJsonSilent(commit.getDiffJson()));
            result.put("createdBy", commit.getCreatedBy());
            result.put("createdDate", formatDate(commit.getCreatedDate()));
            return result;
        });
    }

    private Map<String, Object> doCommit(
            String packCode,
            String packName,
            String commitMessage,
            List<MetaSetModelDto> tables,
            Map<String, List<MetaSetModelDto>> columnsByTable,
            MetadataConnectionConfig connectionConfig
    ) {
        Map<String, MetaSync> latestActiveByTable = loadLatestActiveByPackCode(packCode);

        Map<String, String> currentHashes = new LinkedHashMap<>();
        Map<String, MetaSetModelDto> currentTableMap = new LinkedHashMap<>();
        for (MetaSetModelDto table : tables) {
            String tableCode = table.getCode();
            currentTableMap.put(tableCode, table);
            List<MetaSetModelDto> columns = columnsByTable.getOrDefault(tableCode, List.of());
            currentHashes.put(tableCode, codec.toColumnsHash(columns));
        }

        Map<String, List<MetaSetModelDto>> previousColumnsByTable = new LinkedHashMap<>();
        Map<String, String> previousHashes = new LinkedHashMap<>();
        for (Map.Entry<String, MetaSync> entry : latestActiveByTable.entrySet()) {
            previousHashes.put(entry.getKey(), entry.getValue().getHashData());
            previousColumnsByTable.put(entry.getKey(), readColumnsFromMetaSync(entry.getValue()));
        }

        List<String> addedTables = new ArrayList<>();
        List<String> removedTables = new ArrayList<>();
        List<String> modifiedTables = new ArrayList<>();
        Map<String, Object> diffJson = new LinkedHashMap<>();
        List<Map<String, Object>> addedTablesDiff = new ArrayList<>();
        List<Map<String, Object>> modifiedTablesDiff = new ArrayList<>();
        List<Map<String, Object>> removedTablesDiff = new ArrayList<>();

        for (MetaSetModelDto table : tables) {
            String tableCode = table.getCode();
            List<MetaSetModelDto> currentColumns = columnsByTable.getOrDefault(tableCode, List.of());
            String currentHash = currentHashes.get(tableCode);
            String previousHash = previousHashes.get(tableCode);

            if (previousHash == null) {
                addedTables.add(tableCode);
                addedTablesDiff.add(buildAddedTableDiff(tableCode, table.getName(), currentColumns));
                continue;
            }
            if (Objects.equals(previousHash, currentHash)) {
                continue;
            }
            modifiedTables.add(tableCode);
            modifiedTablesDiff.add(buildModifiedTableDiff(
                    tableCode,
                    table.getName(),
                    previousColumnsByTable.getOrDefault(tableCode, List.of()),
                    currentColumns
            ));
        }

        for (Map.Entry<String, MetaSync> entry : latestActiveByTable.entrySet()) {
            if (!currentTableMap.containsKey(entry.getKey())) {
                removedTables.add(entry.getKey());
                removedTablesDiff.add(buildRemovedTableDiff(entry.getKey(), previousColumnsByTable.getOrDefault(entry.getKey(), List.of())));
            }
        }

        boolean changed = !addedTables.isEmpty() || !removedTables.isEmpty() || !modifiedTables.isEmpty();
        if (!changed) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("changed", false);
            response.put("savedCount", 0);
            return response;
        }

        int newCommitVersionNo = findMaxCommitVersionNoByPackCode(packCode) + 1;

        for (String tableCode : addedTables) {
            int nextVer = findMaxSyncVersionForTable(packCode, tableCode) + 1;
            insertMetaSyncRow(packCode, packName, tableCode, currentTableMap.get(tableCode),
                    columnsByTable.getOrDefault(tableCode, List.of()),
                    currentHashes.get(tableCode), nextVer, false, connectionConfig);
        }
        for (String tableCode : modifiedTables) {
            int nextVer = findMaxSyncVersionForTable(packCode, tableCode) + 1;
            insertMetaSyncRow(packCode, packName, tableCode, currentTableMap.get(tableCode),
                    columnsByTable.getOrDefault(tableCode, List.of()),
                    currentHashes.get(tableCode), nextVer, false, connectionConfig);
        }
        for (String tableCode : removedTables) {
            int nextVer = findMaxSyncVersionForTable(packCode, tableCode) + 1;
            MetaSync existing = latestActiveByTable.get(tableCode);
            insertMetaSyncRow(packCode, packName, tableCode, null,
                    previousColumnsByTable.getOrDefault(tableCode, List.of()),
                    existing != null ? existing.getHashData() : null,
                    nextVer, true, connectionConfig);
        }

        diffJson.put("addedTables", addedTablesDiff);
        diffJson.put("modifiedTables", modifiedTablesDiff);
        diffJson.put("removedTables", removedTablesDiff);

        Map<String, Object> summaryJson = new LinkedHashMap<>();
        summaryJson.put("added", addedTables.size());
        summaryJson.put("modified", modifiedTables.size());
        summaryJson.put("removed", removedTables.size());
        summaryJson.put("addedTableNames", addedTables);
        summaryJson.put("modifiedTableNames", modifiedTables);
        summaryJson.put("removedTableNames", removedTables);

        MetaSyncCommit commitEntity = dataManager.create(MetaSyncCommit.class);
        commitEntity.setPackCode(packCode);
        commitEntity.setVersionNo(newCommitVersionNo);
        commitEntity.setCommitMessage(commitMessage);
        commitEntity.setDiffJson(writeJsonSilent(diffJson));
        commitEntity.setSummaryJson(writeJsonSilent(summaryJson));
        dataManager.save(commitEntity);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("changed", true);
        response.put("versionNo", newCommitVersionNo);
        response.put("savedCount", addedTables.size() + modifiedTables.size() + removedTables.size());
        response.put("summary", summaryJson);
        return response;
    }

    private void insertMetaSyncRow(
            String packCode,
            String packName,
            String tableCode,
            MetaSetModelDto table,
            List<MetaSetModelDto> columns,
            String hash,
            int versionNo,
            boolean isDeleted,
            MetadataConnectionConfig connectionConfig
    ) {
        String metaSetCode = packCode + "-" + tableCode;
        String displayName = (table != null && table.getName() != null) ? table.getName() : tableCode;
        MetaSet metaSet = findOrCreateMetaSet(metaSetCode, (packName != null ? packName : packCode) + "." + displayName);

        MetaSync sync = dataManager.create(MetaSync.class);
        sync.setMetaSet(metaSet);
        sync.setConnectionConfig(connectionConfig);
        sync.setFieldData(codec.toTableMetaSyncFieldData(tableCode, columns));
        sync.setHashData(hash);
        sync.setSyncVersionNo(versionNo);
        sync.setIsDeleted(isDeleted);
        dataManager.save(sync);
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

    private Map<String, MetaSync> loadLatestActiveByPackCode(String packCode) {
        String codePrefix = packCode + "-%";
        List<MetaSync> rows = dataManager.load(MetaSync.class)
                .query("select e from MetaSync e " +
                        "where e.metaSet.code like :codePrefix " +
                        "and e.syncVersionNo = (" +
                        "  select max(x.syncVersionNo) from MetaSync x where x.metaSet.id = e.metaSet.id" +
                        ")")
                .parameter("codePrefix", codePrefix)
                .list();
        Map<String, MetaSync> result = new LinkedHashMap<>();
        for (MetaSync sync : rows) {
            if (Boolean.TRUE.equals(sync.getIsDeleted())) {
                continue;
            }
            String tableCode = extractTableCode(sync, packCode);
            if (tableCode != null) {
                result.put(tableCode, sync);
            }
        }
        return result;
    }

    private int findMaxCommitVersionNoByPackCode(String packCode) {
        Integer maxFromCommit = dataManager.loadValue(
                "select max(e.versionNo) from MetaSyncCommit e where e.packCode = :packCode",
                Integer.class
        ).parameter("packCode", packCode).optional().orElse(0);
        return maxFromCommit != null ? maxFromCommit : 0;
    }

    private int findMaxSyncVersionForTable(String packCode, String tableCode) {
        String metaSetCode = packCode + "-" + tableCode;
        Integer max = dataManager.loadValue(
                "select max(e.syncVersionNo) from MetaSync e where e.metaSet.code = :code",
                Integer.class
        ).parameter("code", metaSetCode).optional().orElse(0);
        return max != null ? max : 0;
    }

    private String extractTableCode(MetaSync sync, String packCode) {
        if (sync.getMetaSet() == null || sync.getMetaSet().getCode() == null) {
            return null;
        }
        String code = sync.getMetaSet().getCode();
        String prefix = packCode + "-";
        if (!code.startsWith(prefix)) {
            return null;
        }
        return code.substring(prefix.length());
    }

    private List<MetaSetModelDto> readColumnsFromMetaSync(MetaSync sync) {
        if (sync.getFieldData() == null) {
            return List.of();
        }
        try {
            JsonNode root = codec.readStorageTree(sync.getFieldData());
            if (!root.has("metaset") || !root.get("metaset").isArray()) {
                return List.of();
            }
            List<MetaSetModelDto> columns = new ArrayList<>();
            for (JsonNode columnNode : root.get("metaset")) {
                columns.add(codec.storageTreeToValue(columnNode, MetaSetModelDto.class));
            }
            return columns;
        } catch (JsonProcessingException e) {
            return List.of();
        }
    }

    private Map<String, Object> buildAddedTableDiff(String tableCode, String tableName, List<MetaSetModelDto> columns) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tableCode", tableCode);
        entry.put("tableName", tableName);
        entry.put("columns", columns.stream().map(this::columnAsMap).collect(Collectors.toList()));
        return entry;
    }

    private Map<String, Object> buildRemovedTableDiff(String tableCode, List<MetaSetModelDto> previousColumns) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tableCode", tableCode);
        entry.put("columns", previousColumns.stream().map(this::columnAsMap).collect(Collectors.toList()));
        return entry;
    }

    private Map<String, Object> buildModifiedTableDiff(
            String tableCode,
            String tableName,
            List<MetaSetModelDto> previousColumns,
            List<MetaSetModelDto> currentColumns
    ) {
        Map<String, MetaSetModelDto> previousMap = previousColumns.stream()
                .collect(Collectors.toMap(MetaSetModelDto::getCode, c -> c, (a, b) -> a));
        Map<String, MetaSetModelDto> currentMap = currentColumns.stream()
                .collect(Collectors.toMap(MetaSetModelDto::getCode, c -> c, (a, b) -> a));

        List<Map<String, Object>> addedColumns = currentMap.entrySet().stream()
                .filter(e -> !previousMap.containsKey(e.getKey()))
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(e -> columnAsMap(e.getValue()))
                .collect(Collectors.toList());

        List<Map<String, Object>> removedColumns = previousMap.entrySet().stream()
                .filter(e -> !currentMap.containsKey(e.getKey()))
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(e -> columnAsMap(e.getValue()))
                .collect(Collectors.toList());

        List<Map<String, Object>> modifiedColumns = new ArrayList<>();
        for (Map.Entry<String, MetaSetModelDto> entry : currentMap.entrySet()) {
            MetaSetModelDto prev = previousMap.get(entry.getKey());
            if (prev == null) {
                continue;
            }
            MetaSetModelDto curr = entry.getValue();
            List<String> changes = new ArrayList<>();
            if (!Objects.equals(prev.getDataType(), curr.getDataType())) changes.add("dataType");
            if (prev.isNull() != curr.isNull()) changes.add("nullable");
            if (prev.isPrimaryKey() != curr.isPrimaryKey()) changes.add("primaryKey");
            if (!Objects.equals(prev.getName(), curr.getName())) changes.add("name");
            if (!Objects.equals(prev.getComment(), curr.getComment())) changes.add("comment");
            if (changes.isEmpty()) {
                continue;
            }
            Map<String, Object> change = new LinkedHashMap<>();
            change.put("code", curr.getCode());
            change.put("before", columnAsMap(prev));
            change.put("after", columnAsMap(curr));
            change.put("changes", changes);
            modifiedColumns.add(change);
        }
        modifiedColumns.sort(Comparator.comparing(c -> String.valueOf(c.get("code"))));

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tableCode", tableCode);
        entry.put("tableName", tableName);
        entry.put("addedColumns", addedColumns);
        entry.put("removedColumns", removedColumns);
        entry.put("modifiedColumns", modifiedColumns);
        return entry;
    }

    private Map<String, Object> columnAsMap(MetaSetModelDto column) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("code", column.getCode());
        map.put("name", column.getName());
        map.put("dataType", column.getDataType());
        map.put("isNull", column.isNull());
        map.put("isPrimaryKey", column.isPrimaryKey());
        map.put("comment", column.getComment());
        return map;
    }

    private String writeJsonSilent(Object value) {
        try {
            return jsonMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private Object parseJsonSilent(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return jsonMapper.readValue(json, new TypeReference<Object>() {});
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private String formatDate(OffsetDateTime date) {
        return date != null ? date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null;
    }
}
