package com.company.tachconnectjmix.metadata.db.service;

import com.company.tachconnectjmix.metadata.dto.DbConnectionRequest;
import com.company.tachconnectjmix.metadata.dto.MetaPackDto;
import com.company.tachconnectjmix.metadata.dto.MetaSetModelDto;
import com.company.tachconnectjmix.metadata.dto.RelationItemDto;
import com.company.tachconnectjmix.metadata.enums.DatabaseType;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class MetadataJdbcService {

    private final DbConnectionService connectionService;

    public MetadataJdbcService(DbConnectionService connectionService) {
        this.connectionService = connectionService;
    }

    public List<Map<String, Object>> runSelectQuery(DbConnectionRequest request, String sql) {
        if (sql == null || sql.isBlank()) {
            return Collections.emptyList();
        }

        String normalized = sql.stripLeading().toLowerCase();
        if (!normalized.startsWith("select")) {
            throw new IllegalArgumentException("Only SELECT statements are allowed");
        }

        try (Connection connection = connectionService.getConnection(request);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {

            List<Map<String, Object>> result = new ArrayList<>();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                result.add(row);
            }

            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot execute SQL query", e);
        }
    }

    public MetaPackDto buildMetaPack(DbConnectionRequest request) {
        try (Connection connection = connectionService.getConnection(request)) {
            DatabaseMetaData metaData = connection.getMetaData();

            String catalog = resolveCatalog(request);
            String schema = resolveSchema(request);

            List<String> tables = new ArrayList<>();
            try (ResultSet rs = metaData.getTables(catalog, schema, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }

            List<MetaSetModelDto> schemaRows = new ArrayList<>();
            List<RelationItemDto> relations = new ArrayList<>();

            for (String table : tables) {
                schemaRows.add(buildTableRoot(table));

                Map<String, Boolean> primaryKeyMap = loadPrimaryKeyMap(metaData, catalog, schema, table);
                schemaRows.addAll(loadFieldRows(metaData, catalog, schema, table, primaryKeyMap));

                relations.addAll(loadRelationRows(metaData, catalog, schema, table));
            }

            MetaPackDto.MetaPackContent content = new MetaPackDto.MetaPackContent();
            content.setVersion("1.0");
            content.setDataSource(mapDataSource(request.getDatabaseType()));
            content.setSchema(schemaRows);
            content.setRelations(relations);

            MetaPackDto response = new MetaPackDto();
            response.setMetaPack(content);
            return response;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot build metaPack from database metadata", e);
        }
    }

    private MetaSetModelDto buildTableRoot(String table) {
        MetaSetModelDto root = new MetaSetModelDto();
        root.setId(table);
        root.setCode(table);
        root.setName(table);
        root.setDataType("collection");
        root.setPath(table);
        root.setPath_parent(null);
        root.setDescription(null);
        root.setNull(false);
        root.setPrimaryKey(false);
        root.setComment(null);
        return root;
    }

    private List<MetaSetModelDto> loadFieldRows(
            DatabaseMetaData metaData,
            String catalog,
            String schema,
            String table,
            Map<String, Boolean> primaryKeyMap
    ) throws SQLException {
        List<MetaSetModelDto> rows = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(catalog, schema, table, "%")) {
            while (rs.next()) {
                String column = rs.getString("COLUMN_NAME");
                String path = table + "." + column;

                String nullable = rs.getString("IS_NULLABLE");
                boolean isNull = "YES".equalsIgnoreCase(nullable) || "1".equals(nullable);

                String dataType = rs.getString("TYPE_NAME");
                String description = rs.getString("REMARKS");

                MetaSetModelDto row = new MetaSetModelDto();
                row.setId(path);
                row.setCode(column);
                row.setName(column);
                row.setDataType(dataType);
                row.setPath(path);
                row.setPath_parent(table);
                row.setDescription(description);
                row.setNull(isNull);
                row.setPrimaryKey(primaryKeyMap.getOrDefault(column.toLowerCase(), false));
                row.setComment(null);
                rows.add(row);
            }
        }
        return rows;
    }

    private Map<String, Boolean> loadPrimaryKeyMap(
            DatabaseMetaData metaData,
            String catalog,
            String schema,
            String table
    ) throws SQLException {
        Map<String, Boolean> map = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getPrimaryKeys(catalog, schema, table)) {
            while (rs.next()) {
                String column = rs.getString("COLUMN_NAME");
                if (column != null) {
                    map.put(column.toLowerCase(), true);
                }
            }
        }
        return map;
    }

    private List<RelationItemDto> loadRelationRows(
            DatabaseMetaData metaData,
            String catalog,
            String schema,
            String table
    ) throws SQLException {
        List<RelationItemDto> rows = new ArrayList<>();
        try (ResultSet rs = metaData.getImportedKeys(catalog, schema, table)) {
            while (rs.next()) {
                String sourceTable = rs.getString("FKTABLE_NAME");
                String sourceField = rs.getString("FKCOLUMN_NAME");
                String targetTable = rs.getString("PKTABLE_NAME");
                String targetField = rs.getString("PKCOLUMN_NAME");

                if (sourceTable == null || sourceField == null || targetTable == null || targetField == null) {
                    continue;
                }

                RelationItemDto item = new RelationItemDto();
                item.setId(sourceTable + "." + sourceField + "->" + targetTable + "." + targetField);
                item.setSourceTable(sourceTable);
                item.setSourceField(sourceField);
                item.setTargetTable(targetTable);
                item.setTargetField(targetField);
                item.setType("N:1");
                rows.add(item);
            }
        }
        return rows;
    }

    private String mapDataSource(DatabaseType type) {
        if (type == DatabaseType.POSTGRES) {
            return "postgres";
        }
        return "restapi";
    }

    private String resolveCatalog(DbConnectionRequest request) {
        return null;
    }

    private String resolveSchema(DbConnectionRequest request) {
        if (request.getSchema() != null && !request.getSchema().isBlank()) {
            return request.getSchema();
        }

        DatabaseType type = request.getDatabaseType();
        if (type == DatabaseType.POSTGRES) {
            return "public";
        }
        return null;
    }
}
