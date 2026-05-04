package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.entity.metapack.MetaPack;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSetVersion;
import com.fasterxml.jackson.databind.JsonNode;
import io.jmix.core.DataManager;
import io.jmix.core.security.SystemAuthenticator;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class MetaSetManagerService {

    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;
    private final MetaSnapshotCodec codec;

    public MetaSetManagerService(
            DataManager dataManager,
            SystemAuthenticator systemAuthenticator,
            MetaSnapshotCodec codec
    ) {
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
        this.codec = codec;
    }

    public List<Map<String, Object>> listAll() {
        return systemAuthenticator.withSystem(() -> {
            List<MetaSet> metaSets = dataManager.load(MetaSet.class)
                    .query("select e from MetaSet e order by e.code asc")
                    .list();

            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaSet ms : metaSets) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("code", ms.getCode());
                row.put("name", ms.getName());
                row.put("operation", ms.getOperation());
                row.put("status", ms.getStatus());
                row.put("currentVersionNo", ms.getCurrentVersionNo());
                row.put("fieldCount", countFieldsFromLatestVersion(ms.getCode()));
                row.put("lastModifiedDate", formatDate(ms.getLastModifiedDate()));
                result.add(row);
            }
            return result;
        });
    }

    public List<Map<String, Object>> listTablesByPack(String packCode) {
        return systemAuthenticator.withSystem(() -> {
            MetaPack pack = dataManager.load(MetaPack.class)
                    .query("e.code = :code")
                    .parameter("code", packCode)
                    .optional().orElse(null);

            if (pack == null) return List.of();

            List<MetaSet> metaSets = dataManager.load(MetaSet.class)
                    .query("e.metaPack = :pack order by e.code asc")
                    .parameter("pack", pack)
                    .list();

            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaSet ms : metaSets) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("code", ms.getCode());
                row.put("name", ms.getName());
                row.put("status", ms.getStatus());
                row.put("operation", ms.getOperation());
                row.put("currentVersionNo", ms.getCurrentVersionNo());
                row.put("fieldCount", countFieldsFromLatestVersion(ms.getCode()));
                row.put("lastModifiedDate", formatDate(ms.getLastModifiedDate()));
                result.add(row);
            }
            return result;
        });
    }

    public List<Map<String, Object>> listVersions(String metaSetCode) {
        return systemAuthenticator.withSystem(() -> {
            List<MetaSetVersion> versions = dataManager.load(MetaSetVersion.class)
                    .query("select v from MetaSetVersion v where v.metaSet.code = :code order by v.versionNo desc")
                    .parameter("code", metaSetCode)
                    .list();

            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaSetVersion v : versions) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("versionNo", v.getVersionNo());
                row.put("fieldCount", countFieldsFromFieldData(v.getFieldData()));
                row.put("hashData", v.getHashData());
                row.put("createdBy", v.getCreatedBy());
                row.put("createdDate", formatDate(v.getCreatedDate()));
                result.add(row);
            }
            return result;
        });
    }

    public List<Map<String, Object>> getLatestFields(String metaSetCode) {
        return systemAuthenticator.withSystem(() -> {
            Integer maxVer = dataManager.loadValue(
                    "select max(v.versionNo) from MetaSetVersion v where v.metaSet.code = :code",
                    Integer.class
            ).parameter("code", metaSetCode).optional().orElse(null);

            if (maxVer == null) return List.of();

            MetaSetVersion latest = dataManager.load(MetaSetVersion.class)
                    .query("select v from MetaSetVersion v where v.metaSet.code = :code and v.versionNo = :ver")
                    .parameter("code", metaSetCode)
                    .parameter("ver", maxVer)
                    .optional().orElse(null);

            if (latest == null || latest.getFieldData() == null) return List.of();

            return codec.fromCanonicalJson(latest.getFieldData()).stream()
                    .map(f -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("code", f.getCode());
                        row.put("name", f.getName());
                        row.put("dataType", f.getDataType());
                        row.put("isPrimaryKey", f.isPrimaryKey());
                        row.put("isNull", f.isNull());
                        row.put("description", f.getDescription());
                        return row;
                    })
                    .collect(java.util.stream.Collectors.toList());
        });
    }

    public Map<String, Object> updateStatus(String metaSetCode, String newStatus) {
        return systemAuthenticator.withSystem(() -> {
            MetaSet ms = dataManager.load(MetaSet.class)
                    .query("e.code = :code")
                    .parameter("code", metaSetCode)
                    .optional()
                    .orElseThrow(() -> new IllegalArgumentException("MetaSet not found: " + metaSetCode));
            ms.setStatus(newStatus);
            dataManager.save(ms);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("code", metaSetCode);
            result.put("status", newStatus);
            return result;
        });
    }

    public Map<String, Object> updateOperation(String metaSetCode, String operation) {
        return systemAuthenticator.withSystem(() -> {
            MetaSet ms = dataManager.load(MetaSet.class)
                    .query("e.code = :code")
                    .parameter("code", metaSetCode)
                    .optional()
                    .orElseThrow(() -> new IllegalArgumentException("MetaSet not found: " + metaSetCode));
            ms.setOperation(operation);
            dataManager.save(ms);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("code", metaSetCode);
            result.put("operation", operation);
            return result;
        });
    }

    public List<Map<String, Object>> getVersionFields(String metaSetCode, Integer versionNo) {
        return systemAuthenticator.withSystem(() -> {
            MetaSetVersion version = dataManager.load(MetaSetVersion.class)
                    .query("select v from MetaSetVersion v where v.metaSet.code = :code and v.versionNo = :ver")
                    .parameter("code", metaSetCode)
                    .parameter("ver", versionNo)
                    .optional().orElse(null);

            if (version == null || version.getFieldData() == null) return List.of();

            return codec.fromCanonicalJson(version.getFieldData()).stream()
                    .map(f -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("code", f.getCode());
                        row.put("name", f.getName());
                        row.put("dataType", f.getDataType());
                        row.put("isPrimaryKey", f.isPrimaryKey());
                        row.put("isNull", f.isNull());
                        row.put("description", f.getDescription());
                        return row;
                    })
                    .collect(java.util.stream.Collectors.toList());
        });
    }

    public Map<String, Object> setCurrentVersion(String metaSetCode, Integer versionNo) {
        return systemAuthenticator.withSystem(() -> {
            MetaSet ms = dataManager.load(MetaSet.class)
                    .query("e.code = :code")
                    .parameter("code", metaSetCode)
                    .optional()
                    .orElseThrow(() -> new IllegalArgumentException("MetaSet not found: " + metaSetCode));

            MetaSetVersion version = dataManager.load(MetaSetVersion.class)
                    .query("select v from MetaSetVersion v where v.metaSet.code = :code and v.versionNo = :ver")
                    .parameter("code", metaSetCode)
                    .parameter("ver", versionNo)
                    .optional()
                    .orElseThrow(() -> new IllegalArgumentException("Version not found: " + versionNo));

            ms.setCurrentVersionNo(versionNo);
            ms.setCurrentHashData(version.getHashData());
            dataManager.save(ms);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("code", metaSetCode);
            result.put("currentVersionNo", versionNo);
            return result;
        });
    }

    public Map<String, Object> compareVersions(String metaSetCode, Integer fromVer, Integer toVer) {
        return systemAuthenticator.withSystem(() -> {
            List<MetaSetModelDto> fromFields = loadVersionFields(metaSetCode, fromVer);
            List<MetaSetModelDto> toFields = loadVersionFields(metaSetCode, toVer);

            Map<String, MetaSetModelDto> fromMap = new LinkedHashMap<>();
            for (MetaSetModelDto f : fromFields) fromMap.put(f.getCode(), f);
            Map<String, MetaSetModelDto> toMap = new LinkedHashMap<>();
            for (MetaSetModelDto f : toFields) toMap.put(f.getCode(), f);

            List<Map<String, Object>> added = new ArrayList<>();
            List<Map<String, Object>> removed = new ArrayList<>();
            List<Map<String, Object>> changed = new ArrayList<>();

            for (Map.Entry<String, MetaSetModelDto> e : toMap.entrySet()) {
                if (!fromMap.containsKey(e.getKey())) {
                    added.add(fieldToMap(e.getValue()));
                } else {
                    MetaSetModelDto before = fromMap.get(e.getKey());
                    MetaSetModelDto after = e.getValue();
                    if (!java.util.Objects.equals(before.getDataType(), after.getDataType())
                            || before.isNull() != after.isNull()
                            || before.isPrimaryKey() != after.isPrimaryKey()
                            || !java.util.Objects.equals(before.getName(), after.getName())) {
                        Map<String, Object> diff = new LinkedHashMap<>();
                        diff.put("code", e.getKey());
                        diff.put("before", fieldToMap(before));
                        diff.put("after", fieldToMap(after));
                        changed.add(diff);
                    }
                }
            }
            for (Map.Entry<String, MetaSetModelDto> e : fromMap.entrySet()) {
                if (!toMap.containsKey(e.getKey())) removed.add(fieldToMap(e.getValue()));
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("from", fromVer);
            result.put("to", toVer);
            result.put("added", added);
            result.put("removed", removed);
            result.put("changed", changed);
            return result;
        });
    }

    private List<MetaSetModelDto> loadVersionFields(String code, Integer versionNo) {
        MetaSetVersion ver = dataManager.load(MetaSetVersion.class)
                .query("select v from MetaSetVersion v where v.metaSet.code = :code and v.versionNo = :vno")
                .parameter("code", code)
                .parameter("vno", versionNo)
                .optional().orElse(null);
        if (ver == null || ver.getFieldData() == null) return List.of();
        return codec.fromCanonicalJson(ver.getFieldData());
    }

    private Map<String, Object> fieldToMap(MetaSetModelDto f) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("code", f.getCode());
        m.put("name", f.getName());
        m.put("dataType", f.getDataType());
        m.put("isPrimaryKey", f.isPrimaryKey());
        m.put("isNull", f.isNull());
        m.put("description", f.getDescription());
        return m;
    }

    private int countFieldsFromLatestVersion(String metaSetCode) {
        Integer maxVer = dataManager.loadValue(
                "select max(v.versionNo) from MetaSetVersion v where v.metaSet.code = :code",
                Integer.class
        ).parameter("code", metaSetCode).optional().orElse(null);

        if (maxVer == null) return 0;

        MetaSetVersion latest = dataManager.load(MetaSetVersion.class)
                .query("select v from MetaSetVersion v where v.metaSet.code = :code and v.versionNo = :ver")
                .parameter("code", metaSetCode)
                .parameter("ver", maxVer)
                .optional()
                .orElse(null);

        return latest != null ? countFieldsFromFieldData(latest.getFieldData()) : 0;
    }

    private int countFieldsFromFieldData(String fieldData) {
        if (fieldData == null || fieldData.isBlank()) return 0;
        try {
            JsonNode root = codec.readStorageTree(fieldData);
            if (root.has("metaset") && root.get("metaset").isArray()) {
                return root.get("metaset").size();
            }
        } catch (Exception ignored) {
        }
        return 0;
    }

    private String formatDate(OffsetDateTime dt) {
        if (dt == null) return null;
        return dt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
