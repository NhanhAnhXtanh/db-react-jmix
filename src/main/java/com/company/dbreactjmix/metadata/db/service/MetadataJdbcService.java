package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.RelationItemDto;
import com.company.dbreactjmix.metadata.enums.DatabaseType;
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

    private void validateSelectOnly(String sql) {
        String normalized = sql.strip().replaceAll("\\s+", " ").toLowerCase();

        if (!normalized.startsWith("select") && !normalized.startsWith("with")) {
            throw new IllegalArgumentException("Only SELECT queries are allowed");
        }

        // Chặn multi-statement
        String stripped = normalized.replaceAll(";\\s*$", "");
        if (stripped.contains(";")) {
            throw new IllegalArgumentException("Multiple SQL statements are not allowed");
        }

        // Chặn DML/DDL dù ở bất kỳ vị trí nào
        String[] forbidden = {"insert ", "update ", "delete ", "drop ", "alter ",
                              "create ", "truncate ", "execute ", "exec "};
        for (String keyword : forbidden) {
            if (normalized.contains(keyword)) {
                throw new IllegalArgumentException("Forbidden SQL keyword: " + keyword.trim());
            }
        }
    }

    public List<Map<String, Object>> runSelectQuery(DbConnectionRequest request, String sql) {
        if (sql == null || sql.isBlank()) {
            return Collections.emptyList();
        }

        validateSelectOnly(sql);

        try (Connection connection = connectionService.getConnection(request);
             Statement statement = connection.createStatement()) {

            statement.setQueryTimeout(30);
            statement.setMaxRows(10000);

            try (ResultSet rs = statement.executeQuery(sql)) {
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
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public MetaPackDto readDatabaseSchema(DbConnectionRequest request) {
        String schema = resolveSchema(request);
        try (Connection connection = connectionService.getConnection(request)) {
            if (schema != null && !schema.isBlank()) {
                connection.setSchema(schema);
            }

            DatabaseMetaData metaData = connection.getMetaData();

            List<String> tables = new ArrayList<>();
            try (ResultSet rs = metaData.getTables(null, schema, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }

            List<MetaSetModelDto> schemaRows = new ArrayList<>();
            List<RelationItemDto> relations = new ArrayList<>();

            for (String table : tables) {
                schemaRows.add(buildTableRoot(table));

                Map<String, Boolean> primaryKeyMap = loadPrimaryKeyMap(metaData, schema, table);
                schemaRows.addAll(loadFieldRows(metaData, schema, table, primaryKeyMap));

                relations.addAll(loadRelationRows(metaData, schema, table));
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
            throw new IllegalStateException(
                    "Không đọc được metadata từ database '" + request.getDbName() + "' / schema '" + schema + "': " + e.getMessage(),
                    e
            );
        }
    }

    public MetaPackDto buildMetaPack(DbConnectionRequest request) {
        return readDatabaseSchema(request);
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
            String schema,
            String table,
            Map<String, Boolean> primaryKeyMap
    ) throws SQLException {
        List<MetaSetModelDto> rows = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(null, schema, table, "%")) {
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
            String schema,
            String table
    ) throws SQLException {
        Map<String, Boolean> map = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getPrimaryKeys(null, schema, table)) {
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
            String schema,
            String table
    ) throws SQLException {
        List<RelationItemDto> rows = new ArrayList<>();
        try (ResultSet rs = metaData.getImportedKeys(null, schema, table)) {
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
                item.setSourceField(sourceTable + "." + sourceField);
                item.setTargetTable(targetTable);
                item.setTargetField(targetTable + "." + targetField);
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
        if (type == DatabaseType.MONGODB) {
            return "mongodb";
        }
        if (type == DatabaseType.RESTAPI) {
            return "restapi";
        }
        return "postgres";
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
