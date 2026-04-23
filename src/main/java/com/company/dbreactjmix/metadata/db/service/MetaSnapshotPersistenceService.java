package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.entity.metapack.MetaPack;
import com.company.dbreactjmix.metadata.entity.metapack.MetaPackVersion;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSetVersion;
import io.jmix.core.DataManager;
import io.jmix.core.SaveContext;
import io.jmix.core.security.SystemAuthenticator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class MetaSnapshotPersistenceService {

    private final DataManager dataManager;
    private final SystemAuthenticator systemAuthenticator;
    private final MetaSnapshotCodec codec;

    public MetaSnapshotPersistenceService(
            DataManager dataManager,
            SystemAuthenticator systemAuthenticator,
            MetaSnapshotCodec codec
    ) {
        this.dataManager = dataManager;
        this.systemAuthenticator = systemAuthenticator;
        this.codec = codec;
    }

    @Transactional
    public Map<String, Object> saveSnapshot(SaveMetaPackRequest request) {
        MetaPackDto.MetaPackContent content = request.getMetaPack();
        List<MetaSetModelDto> allSchema = content.getSchema() != null ? content.getSchema() : List.of();

        List<MetaSetModelDto> tables = allSchema.stream()
                .filter(field -> field.getPath_parent() == null)
                .collect(Collectors.toList());

        Map<String, List<MetaSetModelDto>> columnsByTable = allSchema.stream()
                .filter(field -> field.getPath_parent() != null)
                .collect(Collectors.groupingBy(MetaSetModelDto::getPath_parent));

        List<Map<String, Object>> tableResults = new ArrayList<>();
        int changedCount = 0;

        for (MetaSetModelDto table : tables) {
            List<MetaSetModelDto> columns = columnsByTable.getOrDefault(table.getCode(), List.of());
            String tableCode = request.getMetaSetCode() + "-" + table.getCode();
            String tableName = request.getMetaSetName() + "." + (table.getName() != null ? table.getName() : table.getCode());

            Map<String, Object> result = saveTableSnapshot(tableCode, tableName, table.getCode(), columns);
            tableResults.add(result);
            if (Boolean.TRUE.equals(result.get("changed"))) {
                changedCount++;
            }
        }

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
        return systemAuthenticator.withSystem(() -> {
            List<MetaPackVersion> versions = dataManager.load(MetaPackVersion.class)
                    .query("e.metaPack.code = :code order by e.versionNo desc")
                    .parameter("code", packCode)
                    .list();
            List<Map<String, Object>> result = new ArrayList<>();
            for (MetaPackVersion version : versions) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", version.getId());
                item.put("versionNo", version.getVersionNo());
                item.put("hash", version.getHashData());
                item.put("createdDate", version.getCreatedDate());
                result.add(item);
            }
            return result;
        });
    }

    public List<Map<String, Object>> listVersions(String metaSetCode) {
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
        return systemAuthenticator.withSystem(() -> {
            MetaSetVersion version = dataManager.load(MetaSetVersion.class)
                    .query("e.metaSet.code = :code and e.versionNo = :versionNo")
                    .parameter("code", metaSetCode)
                    .parameter("versionNo", versionNo)
                    .optional()
                    .orElseThrow(() -> new IllegalArgumentException("MetaSet version not found"));

            List<MetaSetModelDto> columns = codec.fromCanonicalJson(version.getFieldData());
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("metaSetCode", metaSetCode);
            response.put("versionId", version.getId());
            response.put("versionNo", version.getVersionNo());
            response.put("hash", version.getHashData());
            response.put("columns", columns);
            return response;
        });
    }

    public MetaPackDto getLatestPackSchema(String packCode) {
        return systemAuthenticator.withSystem(() -> {
            MetaPack pack = dataManager.load(MetaPack.class)
                    .query("e.code = :code")
                    .parameter("code", packCode)
                    .optional()
                    .orElse(null);
            if (pack == null) {
                return null;
            }

            MetaPackVersion latest = findLatestPackVersion(pack);
            if (latest == null || latest.getFieldData() == null) {
                return null;
            }

            try {
                var root = codec.readStorageTree(latest.getFieldData());
                String dataSource = codec.defaultDataSource(root);

                List<com.company.dbreactjmix.metadata.dto.RelationItemDto> relations = new ArrayList<>();
                if (root.has("relations") && root.get("relations").isArray()) {
                    for (var relationNode : root.get("relations")) {
                        relations.add(codec.storageTreeToValue(relationNode, com.company.dbreactjmix.metadata.dto.RelationItemDto.class));
                    }
                }

                List<MetaSetModelDto> schema = new ArrayList<>();
                if (root.has("tables") && root.get("tables").isArray()) {
                    for (var tableNode : root.get("tables")) {
                        String tableName = tableNode.get("table").asText();
                        MetaSetModelDto tableRow = new MetaSetModelDto();
                        tableRow.setCode(tableName);
                        tableRow.setId(tableName);
                        tableRow.setPath(tableName);
                        tableRow.setName(tableName);
                        schema.add(tableRow);
                        if (tableNode.has("metasetdata") && tableNode.get("metasetdata").isArray()) {
                            for (var columnNode : tableNode.get("metasetdata")) {
                                MetaSetModelDto column = codec.storageTreeToValue(columnNode, MetaSetModelDto.class);
                                String fullPath = tableName + "." + column.getCode();
                                column.setId(fullPath);
                                column.setPath(fullPath);
                                column.setPath_parent(tableName);
                                schema.add(column);
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

    private Map<String, Object> saveTableSnapshot(String code, String name, String tableCode, List<MetaSetModelDto> columns) {
        String hash = codec.toColumnsHash(columns);

        return systemAuthenticator.withSystem(() -> {
            MetaSet metaSet = findOrCreateMetaSet(code, name);
            MetaSetVersion latestVersion = findLatestVersion(metaSet);
            boolean changed = latestVersion == null || !hash.equals(latestVersion.getHashData());

            MetaSetVersion savedVersion = latestVersion;
            if (changed) {
                int nextVersionNo = latestVersion == null ? 1 : latestVersion.getVersionNo() + 1;
                String fieldData = codec.toTableFieldData(tableCode, nextVersionNo, columns);
                savedVersion = dataManager.create(MetaSetVersion.class);
                savedVersion.setMetaSet(metaSet);
                savedVersion.setFieldData(fieldData);
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
            result.put("hash", hash);
            return result;
        });
    }

    private Map<String, Object> saveMetaPackSnapshot(
            String packCode,
            String packName,
            MetaPackDto.MetaPackContent content,
            List<MetaSetModelDto> tables,
            Map<String, List<MetaSetModelDto>> columnsByTable
    ) {
        String hash = codec.toPackHash(content);

        return systemAuthenticator.withSystem(() -> {
            MetaPack metaPack = findOrCreateMetaPack(packCode, packName);
            MetaPackVersion latestVersion = findLatestPackVersion(metaPack);
            boolean changed = latestVersion == null || !hash.equals(latestVersion.getHashData());

            MetaPackVersion savedVersion = latestVersion;
            if (changed) {
                int nextVersionNo = latestVersion == null ? 1 : latestVersion.getVersionNo() + 1;
                String fieldData = codec.toPackFieldData(nextVersionNo, content, tables, columnsByTable);
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
            linkMetaSetsToPack(metaPack, packCode, tables);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("metaPackCode", packCode);
            result.put("changed", changed);
            result.put("versionNo", savedVersion != null ? savedVersion.getVersionNo() : null);
            return result;
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

    private void linkMetaSetsToPack(MetaPack metaPack, String packCode, List<MetaSetModelDto> tables) {
        Set<String> metaSetCodes = tables.stream()
                .map(table -> packCode + "-" + table.getCode())
                .collect(Collectors.toSet());
        if (metaSetCodes.isEmpty()) {
            return;
        }

        List<MetaSet> metaSets = dataManager.load(MetaSet.class)
                .query("e.code in :codes")
                .parameter("codes", metaSetCodes)
                .list();

        for (MetaSet metaSet : metaSets) {
            metaSet.setMetaPack(metaPack);
        }
        if (!metaSets.isEmpty()) {
            dataManager.save(new SaveContext().saving(metaSets));
        }
    }
}
