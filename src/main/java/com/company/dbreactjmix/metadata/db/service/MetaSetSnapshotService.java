package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.RelationItemDto;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSetVersion;
import com.fasterxml.jackson.core.JsonProcessingException;
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

    @Transactional
    public Map<String, Object> saveSnapshot(SaveMetaPackRequest request) {
        validateSaveRequest(request);

        MetaPackDto metaPack = request.getMetaPack();
        String canonicalJson = toCanonicalJson(metaPack);
        String hash = toStructuralHash(metaPack);

        return systemAuthenticator.withSystem(() -> {
            MetaSet metaSet = findOrCreateMetaSet(request.getMetaSetCode(), request.getMetaSetName());
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
            metaSet = dataManager.save(metaSet);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("metaSetId", metaSet.getId());
            response.put("metaSetCode", metaSet.getCode());
            response.put("changed", changed);
            response.put("hash", hash);
            response.put("versionId", savedVersion != null ? savedVersion.getId() : null);
            response.put("versionNo", savedVersion != null ? savedVersion.getVersionNo() : null);
            response.put("currentVersionNo", metaSet.getCurrentVersionNo());
            response.put("metaPack", metaPack);
            return response;
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

            MetaPackDto metaPack = fromCanonicalJson(version.getFieldData());
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("metaSetCode", metaSetCode);
            response.put("versionId", version.getId());
            response.put("versionNo", version.getVersionNo());
            response.put("hash", version.getHashData());
            response.put("metaPack", metaPack);
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
        metaSet.setDescription("Metadata snapshot for " + (name != null ? name : code));
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

    // Hash chỉ dùng structural fields: bỏ qua description, comment
    private String toStructuralHash(MetaPackDto metaPack) {
        MetaPackDto.MetaPackContent content = metaPack.getMetaPack();
        if (content == null) {
            return sha256("{}");
        }

        List<Map<String, Object>> structuralSchema = content.getSchema() == null ? List.of()
                : content.getSchema().stream()
                        .<Map<String, Object>>map(this::toStructuralSchemaEntry)
                        .sorted(Comparator.comparing(m -> String.valueOf(m.get("id"))))
                        .collect(Collectors.toList());

        List<Map<String, Object>> structuralRelations = content.getRelations() == null ? List.of()
                : content.getRelations().stream()
                        .<Map<String, Object>>map(this::toStructuralRelationEntry)
                        .sorted(Comparator.comparing(m -> String.valueOf(m.get("id"))))
                        .collect(Collectors.toList());

        Map<String, Object> structural = new TreeMap<>();
        structural.put("relations", structuralRelations);
        structural.put("schema", structuralSchema);

        try {
            return sha256(objectMapper.writeValueAsString(structural));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot compute structural hash", e);
        }
    }

    private Map<String, Object> toStructuralSchemaEntry(MetaSetModelDto field) {
        Map<String, Object> s = new TreeMap<>();
        s.put("code", field.getCode());
        s.put("dataType", field.getDataType());
        s.put("id", field.getId());
        s.put("isNull", field.isNull());
        s.put("isPrimaryKey", field.isPrimaryKey());
        s.put("name", field.getName());
        s.put("path", field.getPath());
        s.put("path_parent", field.getPath_parent());
        return s;
    }

    private Map<String, Object> toStructuralRelationEntry(RelationItemDto rel) {
        Map<String, Object> r = new TreeMap<>();
        r.put("id", rel.getId());
        r.put("sourceField", rel.getSourceField());
        r.put("sourceTable", rel.getSourceTable());
        r.put("targetField", rel.getTargetField());
        r.put("targetTable", rel.getTargetTable());
        r.put("type", rel.getType());
        return r;
    }

    private String toCanonicalJson(Object value) {
        try {
            JsonNode sortedNode = sortNode(objectMapper.valueToTree(value));
            return objectMapper.writeValueAsString(sortedNode);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize metadata snapshot", e);
        }
    }

    private MetaPackDto fromCanonicalJson(String json) {
        try {
            return objectMapper.readValue(json, MetaPackDto.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize metadata snapshot", e);
        }
    }

    private JsonNode sortNode(JsonNode node) {
        if (node == null || node.isNull() || node.isValueNode()) {
            return node;
        }

        if (node.isObject()) {
            Map<String, JsonNode> ordered = new TreeMap<>();
            node.fields().forEachRemaining(entry -> ordered.put(entry.getKey(), sortNode(entry.getValue())));
            return objectMapper.valueToTree(ordered);
        }

        if (node.isArray()) {
            List<JsonNode> items = new ArrayList<>();
            node.forEach(child -> items.add(sortNode(child)));
            items.sort(Comparator.comparing(this::safeCanonicalNode));
            return objectMapper.valueToTree(items);
        }

        return node;
    }

    private String safeCanonicalNode(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot canonicalize metadata node", e);
        }
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte item : bytes) {
                builder.append(String.format("%02x", item));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }

    private void validateSaveRequest(SaveMetaPackRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request is required");
        }
        if (request.getMetaSetCode() == null || request.getMetaSetCode().isBlank()) {
            throw new IllegalArgumentException("metaSetCode is required");
        }
        if (request.getMetaPack() == null || request.getMetaPack().getMetaPack() == null) {
            throw new IllegalArgumentException("metaPack is required");
        }
    }
}
