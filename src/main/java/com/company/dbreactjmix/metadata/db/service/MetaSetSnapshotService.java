package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.entity.metaset.MetaPack;
import com.company.dbreactjmix.metadata.entity.metaset.MetaPackVersion;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSetVersion;
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
import java.util.TreeMap;
import java.util.stream.Collectors;

@Service
public class MetaSetSnapshotService {

    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;
    private final ObjectMapper objectMapper;

    public MetaSetSnapshotService(DataManager dataManager, SystemAuthenticator systemAuthenticator) {
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
        this.objectMapper = JsonMapper.builder()
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .build();
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

            Map<String, Object> result = saveTableSnapshot(tableCode, tableName, columns);
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

    private Map<String, Object> saveTableSnapshot(String code, String name, List<MetaSetModelDto> columns) {
        String canonicalJson = toCanonicalJson(columns);
        String hash = toColumnsHash(columns);

        return systemAuthenticator.withSystem(() -> {
            MetaSet metaSet = findOrCreateMetaSet(code, name);
            MetaSetVersion latestVersion = findLatestVersion(metaSet);
            boolean changed = latestVersion == null || !hash.equals(latestVersion.getHashData());

            MetaSetVersion savedVersion = latestVersion;
            if (changed) {
                savedVersion = dataManager.create(MetaSetVersion.class);
                savedVersion.setMetaSet(metaSet);
                savedVersion.setFieldData(canonicalJson);
                savedVersion.setHashData(hash);
                savedVersion.setVersionNo(latestVersion == null ? 1 : latestVersion.getVersionNo() + 1);
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

        String canonicalJson = toCanonicalJson(content);
        String hash = toPackHash(content);

        return systemAuthenticator.withSystem(() -> {
            MetaPack metaPack = findOrCreateMetaPack(packCode, packName);
            MetaPackVersion latestVersion = findLatestPackVersion(metaPack);
            boolean changed = latestVersion == null || !hash.equals(latestVersion.getHashData());

            MetaPackVersion savedVersion = latestVersion;
            if (changed) {
                savedVersion = dataManager.create(MetaPackVersion.class);
                savedVersion.setMetaPack(metaPack);
                savedVersion.setFieldData(canonicalJson);
                savedVersion.setHashData(hash);
                savedVersion.setVersionNo(latestVersion == null ? 1 : latestVersion.getVersionNo() + 1);
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

    private List<MetaSetModelDto> fromCanonicalJson(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<MetaSetModelDto>>() {});
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

    private void validateSaveRequest(SaveMetaPackRequest request) {
        if (request == null) throw new IllegalArgumentException("Request is required");
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank())
            throw new IllegalArgumentException("metaSetCode is required");
        if (request.getMetaPack() == null)
            throw new IllegalArgumentException("metaPack is required");
    }
}
