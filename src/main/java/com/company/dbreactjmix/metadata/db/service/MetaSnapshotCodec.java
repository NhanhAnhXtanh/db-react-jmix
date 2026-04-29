package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.springframework.stereotype.Component;

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

@Component
public class MetaSnapshotCodec {

    private final ObjectMapper objectMapper;
    private final ObjectMapper storageMapper;

    public MetaSnapshotCodec() {
        this.objectMapper = JsonMapper.builder()
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .build();
        this.storageMapper = new ObjectMapper();
    }

    public String toColumnsHash(List<MetaSetModelDto> columns) {
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

    public String toPackHash(MetaPackDto.MetaPackContent content) {
        List<MetaSetModelDto> schema = content.getSchema() != null ? content.getSchema() : List.of();
        List<Map<String, Object>> structural = schema.stream()
                .filter(field -> field.getPath_parent() != null)
                .<Map<String, Object>>map(this::toStructuralEntry)
                .sorted(Comparator.comparing(m -> String.valueOf(m.get("code"))))
                .collect(Collectors.toList());
        try {
            return sha256(objectMapper.writeValueAsString(structural));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot compute pack hash", e);
        }
    }

    public String toPackFieldData(
            int versionNo,
            MetaPackDto.MetaPackContent content,
            List<MetaSetModelDto> tables,
            Map<String, List<MetaSetModelDto>> columnsByTable
    ) {
        List<Map<String, Object>> tableList = tables.stream().map(table -> {
            List<MetaSetModelDto> columns = columnsByTable.getOrDefault(table.getCode(), List.of());
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("table", table.getCode());
            item.put("metasetdata", toStoredColumnEntries(columns));
            return item;
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

    public String toTableFieldData(String tableCode, int versionNo, List<MetaSetModelDto> columns) {
        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("table", tableCode);
        wrapper.put("versionNo", versionNo);
        wrapper.put("metasetdata", toStoredColumnEntries(columns));
        try {
            return storageMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize table field data", e);
        }
    }

    public String toTableMetaSyncFieldData(String tableCode, List<MetaSetModelDto> columns) {
        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("table", tableCode);
        wrapper.put("metaset", toStoredColumnEntries(columns));
        try {
            return storageMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize table MetaSync field data", e);
        }
    }

    public List<MetaSetModelDto> fromCanonicalJson(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            if (root.isArray()) {
                return objectMapper.readValue(json, new TypeReference<List<MetaSetModelDto>>() {});
            }
            JsonNode metasetdata = root.get("metasetdata");
            if (metasetdata == null || !metasetdata.isArray()) {
                return List.of();
            }
            return objectMapper.readValue(metasetdata.toString(), new TypeReference<List<MetaSetModelDto>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize metadata snapshot", e);
        }
    }

    public JsonNode readStorageTree(String json) throws JsonProcessingException {
        return storageMapper.readTree(json);
    }

    public <T> T storageTreeToValue(JsonNode node, Class<T> type) throws JsonProcessingException {
        return storageMapper.treeToValue(node, type);
    }

    public Map<String, Object> storageTreeToMap(JsonNode node) {
        return storageMapper.convertValue(node, new TypeReference<Map<String, Object>>() {});
    }

    public String defaultDataSource(JsonNode root) {
        return root.has("dataSource") ? root.get("dataSource").asText("postgres") : "postgres";
    }

    private Map<String, Object> toStructuralEntry(MetaSetModelDto field) {
        Map<String, Object> entry = new TreeMap<>();
        entry.put("code", field.getCode());
        entry.put("dataType", field.getDataType());
        entry.put("isNull", field.isNull());
        entry.put("isPrimaryKey", field.isPrimaryKey());
        entry.put("name", field.getName());
        return entry;
    }

    private Map<String, Object> toStoredColumnEntry(MetaSetModelDto column) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("code", column.getCode());
        entry.put("comment", column.getComment());
        entry.put("dataType", column.getDataType());
        entry.put("description", column.getDescription());
        entry.put("id", column.getCode());
        entry.put("isNull", column.isNull());
        entry.put("isPrimaryKey", column.isPrimaryKey());
        entry.put("name", column.getName());
        entry.put("path", column.getCode());
        entry.put("path_parent", null);
        return entry;
    }

    private List<Map<String, Object>> toStoredColumnEntries(List<MetaSetModelDto> columns) {
        return columns.stream()
                .map(this::toStoredColumnEntry)
                .collect(Collectors.toList());
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
            for (byte item : bytes) {
                builder.append(String.format("%02x", item));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
