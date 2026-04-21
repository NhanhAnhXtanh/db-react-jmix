package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.RelationItemDto;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.dto.SyncCheckRequest;
import com.company.dbreactjmix.metadata.dto.SyncConfirmRequest;
import com.company.dbreactjmix.metadata.entity.MetadataConnectionConfig;
import com.company.dbreactjmix.metadata.entity.metaset.MetaPack;
import com.company.dbreactjmix.metadata.entity.metaset.MetaPackVersion;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSetVersion;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSync;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.jmix.core.DataManager;
import io.jmix.core.security.SystemAuthenticator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Service
public class MetaSetSnapshotService {

    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;
    private final MetadataJdbcService metadataJdbcService;
    private final ObjectMapper objectMapper;
    private final ObjectMapper storageMapper;

    public MetaSetSnapshotService(DataManager dataManager, SystemAuthenticator systemAuthenticator,
                                  MetadataJdbcService metadataJdbcService) {
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
        this.metadataJdbcService = metadataJdbcService;
        this.objectMapper = JsonMapper.builder()
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .build();
        this.storageMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    }

    // Mỗi table trong schema = 1 MetaSet riêng
    // fieldData chỉ lưu columns của table đó
    @Transactional
    public Map<String, Object> saveSnapshot(SaveMetaPackRequest request) {
        validateSaveRequest(request);

        MetaPackDto.MetaPackContent content = request.getMetaPack();
        List<MetaSetModelDto> allSchema = content.getSchema() != null ? content.getSchema() : List.of();

        List<MetaSetModelDto> tables = allSchema.stream()
                .filter(f -> f.getPath_parent() == null)
                .collect(Collectors.toList());

        Map<String, List<MetaSetModelDto>> columnsByTable = allSchema.stream()
                .filter(f -> f.getPath_parent() != null)
                .collect(Collectors.groupingBy(MetaSetModelDto::getPath_parent));

        // 1. Lưu từng table → MetaSet + MetaSetVersion
        List<Map<String, Object>> tableResults = new ArrayList<>();
        int changedCount = 0;

        for (MetaSetModelDto table : tables) {
            List<MetaSetModelDto> columns = columnsByTable.getOrDefault(table.getCode(), List.of());
            String tableCode = request.getMetaSetCode() + "-" + table.getCode();
            String tableName = request.getMetaSetName() + "." + (table.getName() != null ? table.getName() : table.getCode());

            Map<String, Object> result = saveTableSnapshot(tableCode, tableName, table.getCode(), columns);
            tableResults.add(result);
            if (Boolean.TRUE.equals(result.get("changed"))) changedCount++;
        }

        // 2. Lưu MetaPack + MetaPackVersion (full schema + relations)
        Map<String, Object> packResult = saveMetaPackSnapshot(
                request.getMetaSetCode(), request.getMetaSetName(), content, tables, columnsByTable);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metaSetCode", request.getMetaSetCode());
        response.put("changed", changedCount > 0 || Boolean.TRUE.equals(packResult.get("changed")));
        response.put("savedCount", changedCount);
        response.put("packVersionNo", packResult.get("versionNo"));
        response.put("tables", tableResults);
        return response;
    }

    private Map<String, Object> saveTableSnapshot(String code, String name, String tableCode, List<MetaSetModelDto> columns) {
        String hash = toColumnsHash(columns);

        return systemAuthenticator.withSystem(() -> {
            MetaSet metaSet = findOrCreateMetaSet(code, name);
            MetaSetVersion latestVersion = findLatestVersion(metaSet);
            boolean changed = latestVersion == null || !hash.equals(latestVersion.getHashData());

            MetaSetVersion savedVersion = latestVersion;
            String savedFieldData = latestVersion != null ? latestVersion.getFieldData() : null;
            if (changed) {
                int nextVersionNo = latestVersion == null ? 1 : latestVersion.getVersionNo() + 1;
                savedFieldData = toTableFieldData(tableCode, nextVersionNo, columns);
                savedVersion = dataManager.create(MetaSetVersion.class);
                savedVersion.setMetaSet(metaSet);
                savedVersion.setFieldData(savedFieldData);
                savedVersion.setHashData(hash);
                savedVersion.setVersionNo(nextVersionNo);
                savedVersion = dataManager.save(savedVersion);
            }

            metaSet.setCurrentVersionNo(savedVersion != null ? savedVersion.getVersionNo() : null);
            metaSet.setCurrentHashData(hash);
            dataManager.save(metaSet);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("metaSetCode", code);
            result.put("changed", changed);
            result.put("versionNo", savedVersion != null ? savedVersion.getVersionNo() : null);
            return result;
        });
    }

    private Map<String, Object> saveMetaPackSnapshot(
            String packCode, String packName,
            MetaPackDto.MetaPackContent content,
            List<MetaSetModelDto> tables,
            Map<String, List<MetaSetModelDto>> columnsByTable) {

        String hash = toPackHash(content);

        return systemAuthenticator.withSystem(() -> {
            MetaPack metaPack = findOrCreateMetaPack(packCode, packName);
            MetaPackVersion latestVersion = findLatestPackVersion(metaPack);
            boolean changed = latestVersion == null || !hash.equals(latestVersion.getHashData());

            MetaPackVersion savedVersion = latestVersion;
            String fieldData = latestVersion != null ? latestVersion.getFieldData() : null;
            if (changed) {
                int nextVersionNo = latestVersion == null ? 1 : latestVersion.getVersionNo() + 1;
                fieldData = toPackFieldData(packCode, nextVersionNo, content, tables, columnsByTable);
                savedVersion = dataManager.create(MetaPackVersion.class);
                savedVersion.setMetaPack(metaPack);
                savedVersion.setFieldData(fieldData);
                savedVersion.setHashData(hash);
                savedVersion.setVersionNo(nextVersionNo);
                savedVersion = dataManager.save(savedVersion);
            }

            metaPack.setCurrentVersionNo(savedVersion != null ? savedVersion.getVersionNo() : null);
            metaPack.setCurrentHashData(hash);
            metaPack = dataManager.save(metaPack);

            // Link các MetaSet thuộc pack này
            final MetaPack finalPack = metaPack;
            for (MetaSetModelDto table : tables) {
                String tableCode = packCode + "-" + table.getCode();
                dataManager.load(MetaSet.class)
                        .query("e.code = :code")
                        .parameter("code", tableCode)
                        .optional()
                        .ifPresent(ms -> {
                            ms.setMetaPack(finalPack);
                            dataManager.save(ms);
                        });
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("metaPackCode", packCode);
            result.put("changed", changed);
            result.put("versionNo", savedVersion != null ? savedVersion.getVersionNo() : null);
            return result;
        });
    }

    private MetaPack findOrCreateMetaPack(String code, String name) {
        MetaPack existing = dataManager.load(MetaPack.class)
                .query("e.code = :code")
                .parameter("code", code)
                .optional()
                .orElse(null);

        MetaPack pack = existing != null ? existing : dataManager.create(MetaPack.class);
        pack.setCode(code);
        pack.setName(name != null ? name : code);
        return dataManager.save(pack);
    }

    private MetaPackVersion findLatestPackVersion(MetaPack metaPack) {
        return dataManager.load(MetaPackVersion.class)
                .query("e.metaPack = :pack order by e.versionNo desc")
                .parameter("pack", metaPack)
                .maxResults(1)
                .optional()
                .orElse(null);
    }

    private String toPackHash(MetaPackDto.MetaPackContent content) {
        List<MetaSetModelDto> schema = content.getSchema() != null ? content.getSchema() : List.of();
        List<Map<String, Object>> structural = schema.stream()
                .filter(f -> f.getPath_parent() != null)
                .<Map<String, Object>>map(this::toStructuralEntry)
                .sorted(Comparator.comparing(m -> String.valueOf(m.get("code"))))
                .collect(Collectors.toList());
        try {
            return sha256(objectMapper.writeValueAsString(structural));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot compute pack hash", e);
        }
    }

    public List<Map<String, Object>> listMetaPacks() {
        return systemAuthenticator.withSystem(() -> {
            List<MetaPack> packs = dataManager.load(MetaPack.class)
                    .query("select e from MetaPack e order by e.name asc")
                    .list();
            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaPack pack : packs) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("code", pack.getCode());
                item.put("name", pack.getName());
                item.put("currentVersionNo", pack.getCurrentVersionNo());
                item.put("lastModifiedDate", pack.getLastModifiedDate());
                result.add(item);
            }
            return result;
        });
    }

    public List<Map<String, Object>> listPackVersions(String packCode) {
        if (packCode == null || packCode.isBlank()) {
            throw new IllegalArgumentException("packCode is required");
        }
        return systemAuthenticator.withSystem(() -> {
            List<MetaPackVersion> versions = dataManager.load(MetaPackVersion.class)
                    .query("e.metaPack.code = :code order by e.versionNo desc")
                    .parameter("code", packCode)
                    .list();
            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaPackVersion v : versions) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", v.getId());
                item.put("versionNo", v.getVersionNo());
                item.put("hash", v.getHashData());
                item.put("createdDate", v.getCreatedDate());
                result.add(item);
            }
            return result;
        });
    }

    public List<Map<String, Object>> listVersions(String metaSetCode) {
        if (metaSetCode == null || metaSetCode.isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }

        return systemAuthenticator.withSystem(() -> {
            List<MetaSetVersion> versions = dataManager.load(MetaSetVersion.class)
                    .query("e.metaSet.code = :code order by e.versionNo desc")
                    .parameter("code", metaSetCode)
                    .list();

            List<Map<String, Object>> response = new ArrayList<>();
            for (MetaSetVersion version : versions) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", version.getId());
                item.put("versionNo", version.getVersionNo());
                item.put("hash", version.getHashData());
                item.put("createdDate", version.getCreatedDate());
                item.put("metaSetCode", metaSetCode);
                response.add(item);
            }
            return response;
        });
    }

    public Map<String, Object> getVersion(String metaSetCode, Integer versionNo) {
        if (metaSetCode == null || metaSetCode.isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        if (versionNo == null) {
            throw new IllegalArgumentException("versionNo is required");
        }

        return systemAuthenticator.withSystem(() -> {
            MetaSetVersion version = dataManager.load(MetaSetVersion.class)
                    .query("e.metaSet.code = :code and e.versionNo = :versionNo")
                    .parameter("code", metaSetCode)
                    .parameter("versionNo", versionNo)
                    .optional()
                    .orElseThrow(() -> new IllegalArgumentException("MetaSet version not found"));

            List<MetaSetModelDto> columns = fromCanonicalJson(version.getFieldData());
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("metaSetCode", metaSetCode);
            response.put("versionId", version.getId());
            response.put("versionNo", version.getVersionNo());
            response.put("hash", version.getHashData());
            response.put("columns", columns);
            return response;
        });
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

    private MetaSetVersion findLatestVersion(MetaSet metaSet) {
        return dataManager.load(MetaSetVersion.class)
                .query("e.metaSet = :metaSet order by e.versionNo desc")
                .parameter("metaSet", metaSet)
                .maxResults(1)
                .optional()
                .orElse(null);
    }

    private String toColumnsHash(List<MetaSetModelDto> columns) {
        List<Map<String, Object>> structural = columns.stream()
                .<Map<String, Object>>map(this::toStructuralEntry)
                .sorted(Comparator.comparing(m -> String.valueOf(m.get("code"))))
                .collect(Collectors.toList());
        try {
            return sha256(objectMapper.writeValueAsString(structural));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot compute columns hash", e);
        }
    }

    private Map<String, Object> toStructuralEntry(MetaSetModelDto field) {
        Map<String, Object> s = new TreeMap<>();
        s.put("code", field.getCode());
        s.put("dataType", field.getDataType());
        s.put("isNull", field.isNull());
        s.put("isPrimaryKey", field.isPrimaryKey());
        s.put("name", field.getName());
        return s;
    }

    private String toCanonicalJson(Object value) {
        try {
            JsonNode sortedNode = sortNode(objectMapper.valueToTree(value));
            return objectMapper.writeValueAsString(sortedNode);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize metadata snapshot", e);
        }
    }

    private String toPackFieldData(String packCode, int versionNo, MetaPackDto.MetaPackContent content,
                                    List<MetaSetModelDto> tables, Map<String, List<MetaSetModelDto>> columnsByTable) {
        List<Map<String, Object>> tableList = tables.stream().map(table -> {
            List<MetaSetModelDto> columns = columnsByTable.getOrDefault(table.getCode(), List.of());
            List<Map<String, Object>> simplified = columns.stream().map(col -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("code", col.getCode());
                m.put("comment", col.getComment());
                m.put("dataType", col.getDataType());
                m.put("description", col.getDescription());
                m.put("id", col.getCode());
                m.put("isNull", col.isNull());
                m.put("isPrimaryKey", col.isPrimaryKey());
                m.put("name", col.getName());
                m.put("path", col.getCode());
                m.put("path_parent", null);
                return m;
            }).collect(Collectors.toList());
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("table", table.getCode());
            t.put("metasetdata", simplified);
            return t;
        }).collect(Collectors.toList());

        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("versionNo", versionNo);
        wrapper.put("dataSource", content.getDataSource());
        wrapper.put("tables", tableList);
        wrapper.put("relations", content.getRelations());
        try {
            return storageMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize pack field data", e);
        }
    }

    private String toTableFieldData(String tableCode, int versionNo, List<MetaSetModelDto> columns) {
        List<Map<String, Object>> simplified = columns.stream().map(col -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("code", col.getCode());
            m.put("comment", col.getComment());
            m.put("dataType", col.getDataType());
            m.put("description", col.getDescription());
            m.put("id", col.getCode());
            m.put("isNull", col.isNull());
            m.put("isPrimaryKey", col.isPrimaryKey());
            m.put("name", col.getName());
            m.put("path", col.getCode());
            m.put("path_parent", null);
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("table", tableCode);
        wrapper.put("versionNo", versionNo);
        wrapper.put("metasetdata", simplified);
        try {
            return storageMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize table field data", e);
        }
    }

    private List<MetaSetModelDto> fromCanonicalJson(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root.isArray()) {
                return objectMapper.readValue(json, new TypeReference<List<MetaSetModelDto>>() {});
            }
            JsonNode metasetdata = root.get("metasetdata");
            if (metasetdata == null || !metasetdata.isArray()) return List.of();
            return objectMapper.readValue(metasetdata.toString(), new TypeReference<List<MetaSetModelDto>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize metadata snapshot", e);
        }
    }

    private JsonNode sortNode(JsonNode node) {
        if (node == null || node.isNull() || node.isValueNode()) return node;

        if (node.isObject()) {
            Map<String, JsonNode> ordered = new TreeMap<>();
            node.fields().forEachRemaining(e -> ordered.put(e.getKey(), sortNode(e.getValue())));
            return objectMapper.valueToTree(ordered);
        }

        if (node.isArray()) {
            List<JsonNode> items = new ArrayList<>();
            node.forEach(child -> items.add(sortNode(child)));
            items.sort(Comparator.comparing(this::safeCanonical));
            return objectMapper.valueToTree(items);
        }

        return node;
    }

    private String safeCanonical(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot canonicalize node", e);
        }
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte item : bytes) builder.append(String.format("%02x", item));
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    public MetaPackDto getLatestPackSchema(String packCode) {
        if (packCode == null || packCode.isBlank()) return null;
        return systemAuthenticator.withSystem(() -> {
            MetaPack pack = dataManager.load(MetaPack.class)
                    .query("e.code = :code")
                    .parameter("code", packCode)
                    .optional()
                    .orElse(null);
            if (pack == null) return null;

            MetaPackVersion latest = findLatestPackVersion(pack);
            if (latest == null || latest.getFieldData() == null) return null;

            try {
                com.fasterxml.jackson.databind.JsonNode root = storageMapper.readTree(latest.getFieldData());
                String dataSource = root.has("dataSource") ? root.get("dataSource").asText("postgres") : "postgres";

                List<RelationItemDto> relations = new ArrayList<>();
                if (root.has("relations") && root.get("relations").isArray()) {
                    for (com.fasterxml.jackson.databind.JsonNode rel : root.get("relations")) {
                        relations.add(storageMapper.treeToValue(rel, RelationItemDto.class));
                    }
                }

                List<MetaSetModelDto> schema = new ArrayList<>();
                if (root.has("tables") && root.get("tables").isArray()) {
                    for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("tables")) {
                        String tableName = tableNode.get("table").asText();
                        MetaSetModelDto tableRow = new MetaSetModelDto();
                        tableRow.setCode(tableName);
                        tableRow.setId(tableName);
                        tableRow.setPath(tableName);
                        tableRow.setName(tableName);
                        schema.add(tableRow);
                        if (tableNode.has("metasetdata") && tableNode.get("metasetdata").isArray()) {
                            for (com.fasterxml.jackson.databind.JsonNode colNode : tableNode.get("metasetdata")) {
                                MetaSetModelDto col = storageMapper.treeToValue(colNode, MetaSetModelDto.class);
                                String fullPath = tableName + "." + col.getCode();
                                col.setId(fullPath);
                                col.setPath(fullPath);
                                col.setPath_parent(tableName);
                                schema.add(col);
                            }
                        }
                    }
                }

                MetaPackDto.MetaPackContent content = new MetaPackDto.MetaPackContent();
                content.setVersion("1.0");
                content.setDataSource(dataSource);
                content.setSchema(schema);
                content.setRelations(relations);

                MetaPackDto dto = new MetaPackDto();
                dto.setMetaPack(content);
                return dto;
            } catch (Exception e) {
                return null;
            }
        });
    }

    public Map<String, Object> checkSync(SyncCheckRequest request) {
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank())
            throw new IllegalArgumentException("metaSetCode is required");

        MetaPackDto currentPack = metadataJdbcService.buildMetaPack(request.getConnection());
        MetaPackDto.MetaPackContent content = currentPack.getMetaPack();
        List<MetaSetModelDto> allSchema = content.getSchema() != null ? content.getSchema() : List.of();

        List<MetaSetModelDto> currentTables = allSchema.stream()
                .filter(f -> f.getPath_parent() == null)
                .collect(Collectors.toList());
        Map<String, List<MetaSetModelDto>> currentColsByTable = allSchema.stream()
                .filter(f -> f.getPath_parent() != null)
                .collect(Collectors.groupingBy(MetaSetModelDto::getPath_parent));

        List<Map<String, Object>> diffs = systemAuthenticator.withSystem(() -> {
            List<Map<String, Object>> result = new ArrayList<>();

            // Dùng MetaSync làm baseline
            MetaPack metaPack = dataManager.load(MetaPack.class)
                    .query("e.code = :code")
                    .parameter("code", request.getMetaSetCode())
                    .optional()
                    .orElse(null);

            Map<String, List<MetaSetModelDto>> prevColsByTable = new LinkedHashMap<>();
            if (metaPack != null) {
                // Load per-table MetaSync làm baseline
                List<MetaSet> metaSets = dataManager.load(MetaSet.class)
                        .query("e.metaPack = :pack")
                        .parameter("pack", metaPack)
                        .list();
                for (MetaSet ms : metaSets) {
                    MetaSync latestSync = dataManager.load(MetaSync.class)
                            .query("e.metaSet = :ms order by e.syncVersionNo desc")
                            .parameter("ms", ms)
                            .maxResults(1)
                            .optional()
                            .orElse(null);
                    if (latestSync != null && latestSync.getFieldData() != null) {
                        try {
                            com.fasterxml.jackson.databind.JsonNode root = storageMapper.readTree(latestSync.getFieldData());
                            String tableCode = root.has("table") ? root.get("table").asText() : null;
                            if (tableCode != null && root.has("metaset") && root.get("metaset").isArray()) {
                                List<MetaSetModelDto> cols = new ArrayList<>();
                                for (com.fasterxml.jackson.databind.JsonNode colNode : root.get("metaset")) {
                                    cols.add(storageMapper.treeToValue(colNode, MetaSetModelDto.class));
                                }
                                prevColsByTable.put(tableCode, cols);
                            }
                        } catch (Exception ignored) {}
                    }
                }
                if (prevColsByTable.isEmpty()) {
                    // Fallback MetaPackVersion
                    MetaPackVersion latestVersion = findLatestPackVersion(metaPack);
                    if (latestVersion != null && latestVersion.getFieldData() != null) {
                        prevColsByTable = parsePackSyncColumns(latestVersion.getFieldData());
                    }
                }
            }

            java.util.Set<String> currentTableCodes = currentTables.stream()
                    .map(MetaSetModelDto::getCode)
                    .collect(Collectors.toSet());

            for (MetaSetModelDto table : currentTables) {
                List<MetaSetModelDto> currentCols = currentColsByTable.getOrDefault(table.getCode(), List.of());
                if (!prevColsByTable.containsKey(table.getCode())) {
                    List<String> allCols = currentCols.stream()
                            .map(MetaSetModelDto::getCode).sorted().collect(Collectors.toList());
                    Map<String, Object> diff = new LinkedHashMap<>();
                    diff.put("tableCode", table.getCode());
                    diff.put("isNewTable", true);
                    diff.put("added", allCols);
                    diff.put("removed", List.of());
                    diff.put("changed", List.of());
                    result.add(diff);
                } else {
                    List<MetaSetModelDto> prevCols = prevColsByTable.get(table.getCode());
                    if (!toColumnsHash(currentCols).equals(toColumnsHash(prevCols))) {
                        result.add(computeDiff(table.getCode(), prevCols, currentCols));
                    }
                }
            }

            for (String prevTableCode : prevColsByTable.keySet()) {
                if (!currentTableCodes.contains(prevTableCode)) {
                    Map<String, Object> diff = new LinkedHashMap<>();
                    diff.put("tableCode", prevTableCode);
                    diff.put("isDeletedTable", true);
                    diff.put("added", List.of());
                    diff.put("removed", List.of());
                    diff.put("changed", List.of());
                    result.add(diff);
                }
            }

            return result;
        });

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("hasChanges", !diffs.isEmpty());
        response.put("diffs", diffs);
        return response;
    }

    private Map<String, List<MetaSetModelDto>> parsePackSyncColumns(String fieldData) {
        try {
            com.fasterxml.jackson.databind.JsonNode root = storageMapper.readTree(fieldData);
            Map<String, List<MetaSetModelDto>> result = new LinkedHashMap<>();
            if (root.has("tables") && root.get("tables").isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("tables")) {
                    String tableCode = tableNode.get("table").asText();
                    List<MetaSetModelDto> cols = new ArrayList<>();
                    if (tableNode.has("metasetdata") && tableNode.get("metasetdata").isArray()) {
                        for (com.fasterxml.jackson.databind.JsonNode colNode : tableNode.get("metasetdata")) {
                            cols.add(storageMapper.treeToValue(colNode, MetaSetModelDto.class));
                        }
                    }
                    result.put(tableCode, cols);
                }
            }
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot parse sync fieldData", e);
        }
    }

    public Map<String, Object> confirmSync(SyncConfirmRequest request) {
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank())
            throw new IllegalArgumentException("metaSetCode is required");

        MetaPackDto metaPackDto = metadataJdbcService.buildMetaPack(request.getConnection());
        MetaPackDto.MetaPackContent content = metaPackDto.getMetaPack();
        List<MetaSetModelDto> allSchema = content.getSchema() != null ? content.getSchema() : List.of();
        List<MetaSetModelDto> tables = allSchema.stream()
                .filter(f -> f.getPath_parent() == null)
                .collect(Collectors.toList());
        Map<String, List<MetaSetModelDto>> columnsByTable = allSchema.stream()
                .filter(f -> f.getPath_parent() != null)
                .collect(Collectors.groupingBy(MetaSetModelDto::getPath_parent));

        saveMetaPackSnapshot(request.getMetaSetCode(), request.getMetaSetName(), content, tables, columnsByTable);

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
        result.put("savedCount", tables.size());
        return result;
    }

    // Mỗi lần sync = full snapshot: tạo 1 row MetaSync cho mỗi table hiện tại.
    // syncVersionNo là cấp pack (tất cả table trong cùng 1 sync có cùng version).
    // Nếu không có bất kỳ thay đổi nào (hash trùng, không thêm/xoá table) → không lưu.
    private int upsertMetaSync(String packCode, String packName,
                               List<MetaSetModelDto> tables,
                               Map<String, List<MetaSetModelDto>> columnsByTable,
                               MetadataConnectionConfig connectionConfig) {
        return systemAuthenticator.withSystem(() -> {
            MetaPack metaPack = dataManager.load(MetaPack.class)
                    .query("e.code = :code")
                    .parameter("code", packCode)
                    .optional()
                    .orElse(null);

            // Load tất cả hash hiện có theo tableCode + tính maxSyncNo
            Map<String, String> existingHashes = new LinkedHashMap<>();
            int maxSyncNo = 0;
            if (metaPack != null) {
                List<MetaSet> existingMetaSets = dataManager.load(MetaSet.class)
                        .query("e.metaPack = :pack")
                        .parameter("pack", metaPack)
                        .list();
                for (MetaSet ms : existingMetaSets) {
                    MetaSync latestSync = dataManager.load(MetaSync.class)
                            .query("e.metaSet = :ms order by e.syncVersionNo desc")
                            .parameter("ms", ms)
                            .maxResults(1)
                            .optional()
                            .orElse(null);
                    if (latestSync != null && latestSync.getFieldData() != null) {
                        try {
                            com.fasterxml.jackson.databind.JsonNode root = storageMapper.readTree(latestSync.getFieldData());
                            String tableCode = root.has("table") ? root.get("table").asText() : null;
                            if (tableCode != null) {
                                existingHashes.put(tableCode, latestSync.getHashData());
                            }
                        } catch (Exception ignored) {}
                        if (latestSync.getSyncVersionNo() != null) {
                            maxSyncNo = Math.max(maxSyncNo, latestSync.getSyncVersionNo());
                        }
                    }
                }
            }

            // Tính hash hiện tại của từng table
            Map<String, String> currentHashes = new LinkedHashMap<>();
            for (MetaSetModelDto table : tables) {
                String tableCode = table.getCode();
                List<MetaSetModelDto> columns = columnsByTable.getOrDefault(tableCode, List.of());
                currentHashes.put(tableCode, toColumnsHash(columns));
            }

            // So sánh: có thay đổi nếu khác hash, thêm table mới, hoặc xoá table
            boolean anyChange = !currentHashes.keySet().equals(existingHashes.keySet());
            if (!anyChange) {
                for (Map.Entry<String, String> e : currentHashes.entrySet()) {
                    if (!e.getValue().equals(existingHashes.get(e.getKey()))) {
                        anyChange = true;
                        break;
                    }
                }
            }

            if (!anyChange) {
                return 0; // Không có thay đổi → không lưu version mới
            }

            final int packSyncVersion = maxSyncNo + 1;

            // Lưu tất cả table hiện tại làm 1 full snapshot version
            for (MetaSetModelDto table : tables) {
                String tableCode = table.getCode();
                String metaSetCode = packCode + "-" + tableCode;
                List<MetaSetModelDto> columns = columnsByTable.getOrDefault(tableCode, List.of());
                String hash = currentHashes.get(tableCode);

                MetaSet metaSet = findOrCreateMetaSet(metaSetCode,
                        packName + "." + (table.getName() != null ? table.getName() : tableCode));

                String fieldData = toTableMetaSyncFieldData(tableCode, columns);

                MetaSync sync = dataManager.create(MetaSync.class);
                sync.setMetaSet(metaSet);
                sync.setConnectionConfig(connectionConfig);
                sync.setFieldData(fieldData);
                sync.setHashData(hash);
                sync.setSyncVersionNo(packSyncVersion);
                dataManager.save(sync);
            }
            return tables.size();
        });
    }

    // fieldData: { "table": "users", "metaset": [ {id, code, name, dataType, path, path_parent, ...} ] }
    private String toTableMetaSyncFieldData(String tableCode, List<MetaSetModelDto> columns) {
        List<Map<String, Object>> metaset = columns.stream().map(col -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("code", col.getCode());
            m.put("comment", col.getComment());
            m.put("dataType", col.getDataType());
            m.put("description", col.getDescription());
            m.put("id", col.getCode());
            m.put("isNull", col.isNull());
            m.put("isPrimaryKey", col.isPrimaryKey());
            m.put("name", col.getName());
            m.put("path", col.getCode());
            m.put("path_parent", null);
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("table", tableCode);
        wrapper.put("metaset", metaset);
        try {
            return storageMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize table MetaSync field data", e);
        }
    }

    // Trả về raw fieldData của từng MetaSync row mới nhất: [{ table, metaset: [...] }, ...]
    public List<Map<String, Object>> getLatestMetaSyncSchema(String packCode) {
        if (packCode == null || packCode.isBlank()) return null;
        return systemAuthenticator.withSystem(() -> {
            MetaPack pack = dataManager.load(MetaPack.class)
                    .query("e.code = :code")
                    .parameter("code", packCode)
                    .optional()
                    .orElse(null);
            if (pack == null) return null;

            List<MetaSet> metaSets = dataManager.load(MetaSet.class)
                    .query("e.metaPack = :pack order by e.code asc")
                    .parameter("pack", pack)
                    .list();
            if (metaSets.isEmpty()) return null;

            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaSet ms : metaSets) {
                MetaSync latest = dataManager.load(MetaSync.class)
                        .query("e.metaSet = :ms order by e.syncVersionNo desc")
                        .parameter("ms", ms)
                        .maxResults(1)
                        .optional()
                        .orElse(null);
                if (latest == null || latest.getFieldData() == null) continue;
                try {
                    com.fasterxml.jackson.databind.JsonNode root = storageMapper.readTree(latest.getFieldData());
                    Map<String, Object> tableData = storageMapper.convertValue(root, new TypeReference<Map<String, Object>>() {});
                    result.add(tableData);
                } catch (Exception ignored) {}
            }

            return result.isEmpty() ? null : result;
        });
    }

    private Map<String, Object> computeDiff(String tableCode,
                                             List<MetaSetModelDto> prev,
                                             List<MetaSetModelDto> current) {
        Map<String, MetaSetModelDto> prevMap = prev.stream()
                .collect(Collectors.toMap(MetaSetModelDto::getCode, c -> c, (a, b) -> a));
        Map<String, MetaSetModelDto> currentMap = current.stream()
                .collect(Collectors.toMap(MetaSetModelDto::getCode, c -> c, (a, b) -> a));

        List<String> added = currentMap.keySet().stream()
                .filter(k -> !prevMap.containsKey(k))
                .sorted()
                .collect(Collectors.toList());

        List<String> removed = prevMap.keySet().stream()
                .filter(k -> !currentMap.containsKey(k))
                .sorted()
                .collect(Collectors.toList());

        List<Map<String, Object>> changed = currentMap.entrySet().stream()
                .filter(e -> prevMap.containsKey(e.getKey()))
                .filter(e -> !Objects.equals(e.getValue().getDataType(),
                        prevMap.get(e.getKey()).getDataType()))
                .map(e -> {
                    Map<String, Object> c = new LinkedHashMap<>();
                    c.put("code", e.getKey());
                    c.put("from", prevMap.get(e.getKey()).getDataType());
                    c.put("to", e.getValue().getDataType());
                    return c;
                })
                .collect(Collectors.toList());

        Map<String, Object> diff = new LinkedHashMap<>();
        diff.put("tableCode", tableCode);
        diff.put("added", added);
        diff.put("removed", removed);
        diff.put("changed", changed);
        return diff;
    }

    private void validateSaveRequest(SaveMetaPackRequest request) {
        if (request == null) throw new IllegalArgumentException("Request is required");
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank())
            throw new IllegalArgumentException("metaSetCode is required");
        if (request.getMetaPack() == null)
            throw new IllegalArgumentException("metaPack is required");
    }
}
