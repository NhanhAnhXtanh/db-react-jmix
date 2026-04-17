package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class MetaSetSnapshotService {

    private final MetadataJdbcService metadataJdbcService;
    private final ConnectionConfigService connectionConfigService;
    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;
    private final ObjectMapper objectMapper;

    public MetaSetSnapshotService(
            MetadataJdbcService metadataJdbcService,
            ConnectionConfigService connectionConfigService,
            DataManager dataManager,
            SystemAuthenticator systemAuthenticator
    ) {
        this.metadataJdbcService = metadataJdbcService;
        this.connectionConfigService = connectionConfigService;
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
        this.objectMapper = JsonMapper.builder()
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .build();
    }

    @Transactional
    public Map<String, Object> saveSnapshot(DbConnectionRequest request) {
        validateRequest(request);

        MetaPackDto metaPack = metadataJdbcService.buildMetaPack(request);
        String canonicalJson = toCanonicalJson(metaPack);
        String hash = sha256(canonicalJson);

        return systemAuthenticator.withSystem(() -> {
            MetaSet metaSet = findOrCreateMetaSet(request);
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

    private MetaSet findOrCreateMetaSet(DbConnectionRequest request) {
        String code = buildMetaSetCode(request);
        MetaSet existing = dataManager.load(MetaSet.class)
                .query("e.code = :code")
                .parameter("code", code)
                .optional()
                .orElse(null);

        MetaSet metaSet = existing != null ? existing : dataManager.create(MetaSet.class);
        metaSet.setCode(code);
        metaSet.setName(buildMetaSetName(request));
        metaSet.setDescription("Metadata snapshot for " + buildMetaSetName(request));
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

    private String buildMetaSetCode(DbConnectionRequest request) {
        return connectionConfigService.buildCode(request) + "-" + resolveSchema(request).toLowerCase(Locale.ROOT);
    }

    private String buildMetaSetName(DbConnectionRequest request) {
        return request.getDbName() + "." + resolveSchema(request);
    }

    private String resolveSchema(DbConnectionRequest request) {
        if (request.getSchema() != null && !request.getSchema().isBlank()) {
            return request.getSchema().trim();
        }
        return request.getDatabaseType() != null && "POSTGRES".equals(request.getDatabaseType().name()) ? "public" : "default";
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
            Map<String, JsonNode> ordered = new java.util.TreeMap<>();
            node.fields().forEachRemaining(entry -> ordered.put(entry.getKey(), sortNode(entry.getValue())));
            return objectMapper.valueToTree(ordered);
        }

        if (node.isArray()) {
            java.util.List<JsonNode> items = new java.util.ArrayList<>();
            node.forEach(child -> items.add(sortNode(child)));
            items.sort(java.util.Comparator.comparing(this::safeCanonicalNode));
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

    private void validateRequest(DbConnectionRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Connection request is required");
        }
        if (request.getDatabaseType() == null) {
            throw new IllegalArgumentException("databaseType is required");
        }
        if (request.getDbName() == null || request.getDbName().isBlank()) {
            throw new IllegalArgumentException("dbName is required");
        }
    }
}
