package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.mongodb.ConnectionString;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class MongoMetadataService {

    private static final int SCHEMA_SCAN_BATCH_SIZE = 1000;
    private static final long SCHEMA_CACHE_TTL_MS = 5 * 60 * 1000L;
    private static final ConcurrentMap<String, CachedMetaPack> SCHEMA_CACHE = new ConcurrentHashMap<>();

    private static final Pattern FIND_PATTERN = Pattern.compile(
            "^\\s*db\\.([A-Za-z0-9_\\-]+)\\.find\\s*\\((.*)\\)\\s*;?\\s*$",
            Pattern.DOTALL
    );
    private static final Pattern AGGREGATE_PATTERN = Pattern.compile(
            "^\\s*db\\.([A-Za-z0-9_\\-]+)\\.aggregate\\s*\\((.*)\\)\\s*;?\\s*$",
            Pattern.DOTALL
    );

    public MetaPackDto buildMetaPack(DbConnectionRequest request) {
        String cacheKey = buildSchemaCacheKey(request);
        CachedMetaPack cached = SCHEMA_CACHE.get(cacheKey);
        long now = System.currentTimeMillis();
        if (cached != null && now - cached.createdAtMs() <= SCHEMA_CACHE_TTL_MS) {
            return cached.metaPack();
        }

        try (MongoClient client = createClient(request)) {
            MongoDatabase database = client.getDatabase(request.getDbName());
            List<MetaSetModelDto> schemaRows = new ArrayList<>();

            for (Document collectionInfo : database.listCollections()) {
                String collectionName = collectionInfo.getString("name");
                if (collectionName == null || collectionName.startsWith("system.")) {
                    continue;
                }

                schemaRows.add(buildCollectionRoot(collectionName));
                Document validator = readValidator(collectionInfo);
                Map<String, MetaSetModelDto> collectionRows = new LinkedHashMap<>();
                convertValidator(collectionName, validator).forEach(row -> collectionRows.put(row.getPath(), row));
                scanCollectionFields(collectionName, database.getCollection(collectionName)).forEach(row -> collectionRows.putIfAbsent(row.getPath(), row));
                schemaRows.addAll(collectionRows.values());
            }

            MetaPackDto.MetaPackContent content = new MetaPackDto.MetaPackContent();
            content.setVersion("1.0");
            content.setDataSource("mongodb");
            content.setSchema(schemaRows);
            content.setRelations(List.of());

            MetaPackDto response = new MetaPackDto();
            response.setMetaPack(content);
            SCHEMA_CACHE.put(cacheKey, new CachedMetaPack(response, now));
            return response;
        }
    }

    public List<Map<String, Object>> runReadQuery(DbConnectionRequest request, String query) {
        if (query == null || query.isBlank()) {
            return List.of();
        }
        validateReadOnly(query);

        try (MongoClient client = createClient(request)) {
            MongoDatabase database = client.getDatabase(request.getDbName());
            Matcher findMatcher = FIND_PATTERN.matcher(query);
            if (findMatcher.matches()) {
                String collectionName = findMatcher.group(1);
                Document filter = parseFindFilter(findMatcher.group(2));
                FindIterable<Document> docs = database.getCollection(collectionName)
                        .find(filter)
                        .limit(1000);
                return toRows(docs);
            }

            Matcher aggregateMatcher = AGGREGATE_PATTERN.matcher(query);
            if (aggregateMatcher.matches()) {
                String collectionName = aggregateMatcher.group(1);
                List<Document> pipeline = parsePipeline(aggregateMatcher.group(2));
                return toRows(database.getCollection(collectionName).aggregate(pipeline), 1000);
            }
        }

        throw new IllegalArgumentException("Only db.<collection>.find({...}) and db.<collection>.aggregate([...]) are supported");
    }

    private MongoClient createClient(DbConnectionRequest request) {
        return MongoClients.create(new ConnectionString(buildConnectionString(request)));
    }

    private String buildConnectionString(DbConnectionRequest request) {
        String userInfo = "";
        if (!isBlank(request.getUsername())) {
            userInfo = encode(request.getUsername()) + ":" + encode(request.getPassword() == null ? "" : request.getPassword()) + "@";
        }

        String authSource = "";
        if (!isBlank(request.getSchema())) {
            authSource = "?authSource=" + encode(request.getSchema());
        }

        return "mongodb://" + userInfo + request.getHost() + ":" + request.getPort() + "/" + request.getDbName() + authSource;
    }

    private Document readValidator(Document collectionInfo) {
        Document options = collectionInfo.get("options", Document.class);
        if (options == null) {
            return null;
        }
        return options.get("validator", Document.class);
    }

    private MetaSetModelDto buildCollectionRoot(String collectionName) {
        MetaSetModelDto root = new MetaSetModelDto();
        root.setId(collectionName);
        root.setCode(collectionName);
        root.setName(collectionName);
        root.setDataType("collection");
        root.setPath(collectionName);
        root.setPath_parent(null);
        root.setNull(false);
        root.setPrimaryKey(false);
        return root;
    }

    private List<MetaSetModelDto> convertValidator(String collectionName, Document validator) {
        if (validator == null) {
            return List.of();
        }

        Document jsonSchema = validator.get("$jsonSchema", Document.class);
        if (jsonSchema == null) {
            return List.of();
        }

        Document properties = jsonSchema.get("properties", Document.class);
        Set<String> required = new HashSet<>(readRequired(jsonSchema));
        List<MetaSetModelDto> rows = new ArrayList<>();
        walkProperties(collectionName, collectionName, properties, required, rows);
        return rows;
    }

    private List<MetaSetModelDto> scanCollectionFields(String collectionName, MongoCollection<Document> collection) {
        Map<String, MetaSetModelDto> rows = new LinkedHashMap<>();
        Set<String> seenShapes = new HashSet<>();
        for (Document document : collection.find().batchSize(SCHEMA_SCAN_BATCH_SIZE)) {
            Map<String, MetaSetModelDto> documentRows = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                scanValue(collectionName, collectionName, entry.getKey(), entry.getValue(), documentRows);
            }
            String shapeFingerprint = String.join("|", documentRows.keySet());
            if (seenShapes.add(shapeFingerprint)) {
                documentRows.forEach(rows::putIfAbsent);
            }
        }
        return new ArrayList<>(rows.values());
    }

    private void scanValue(
            String parentPath,
            String pathPrefix,
            String field,
            Object value,
            Map<String, MetaSetModelDto> rows
    ) {
        String path = pathPrefix + "." + field;
        String type = inferType(value);
        rows.putIfAbsent(path, buildFieldRow(field, path, parentPath, type, true));

        if (value instanceof Document document) {
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                scanValue(path, path, entry.getKey(), entry.getValue(), rows);
            }
            return;
        }

        if (value instanceof Collection<?> collection) {
            for (Object item : collection) {
                if (item instanceof Document itemDocument) {
                    for (Map.Entry<String, Object> entry : itemDocument.entrySet()) {
                        scanValue(path, path, entry.getKey(), entry.getValue(), rows);
                    }
                }
            }
        }
    }

    private void walkProperties(
            String parentPath,
            String pathPrefix,
            Document properties,
            Set<String> required,
            List<MetaSetModelDto> rows
    ) {
        if (properties == null) {
            return;
        }

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (!(entry.getValue() instanceof Document definition)) {
                continue;
            }

            String field = entry.getKey();
            String path = pathPrefix + "." + field;
            String type = definition.getString("bsonType");
            if (type == null) {
                type = "object";
            }

            rows.add(buildFieldRow(field, path, parentPath, type, !required.contains(field)));

            if ("object".equals(type)) {
                Set<String> childRequired = new HashSet<>(readRequired(definition));
                walkProperties(path, path, definition.get("properties", Document.class), childRequired, rows);
            } else if ("array".equals(type)) {
                Document items = definition.get("items", Document.class);
                if (items != null && "object".equals(items.getString("bsonType"))) {
                    Set<String> itemRequired = new HashSet<>(readRequired(items));
                    walkProperties(path, path, items.get("properties", Document.class), itemRequired, rows);
                }
            }
        }
    }

    private MetaSetModelDto buildFieldRow(String field, String path, String parentPath, String type, boolean nullable) {
        MetaSetModelDto row = new MetaSetModelDto();
        row.setId(path);
        row.setCode(field);
        row.setName(field);
        row.setDataType(type);
        row.setPath(path);
        row.setPath_parent(parentPath);
        row.setNull(nullable);
        row.setPrimaryKey("_id".equals(field));
        return row;
    }

    private List<String> readRequired(Document schema) {
        List<String> required = schema.getList("required", String.class);
        return required == null ? List.of() : required;
    }

    private String inferType(Object value) {
        if (value instanceof ObjectId) {
            return "objectId";
        }
        if (value instanceof Document) {
            return "object";
        }
        if (value instanceof Collection<?>) {
            return "array";
        }
        if (value instanceof Integer || value instanceof Long || value instanceof Short) {
            return "integer";
        }
        if (value instanceof Number) {
            return "number";
        }
        if (value instanceof Boolean) {
            return "boolean";
        }
        if (value instanceof Date) {
            return "date";
        }
        return "string";
    }

    private void validateReadOnly(String query) {
        String normalized = query.strip().replaceAll("\\s+", " ").toLowerCase();
        String[] forbidden = {"insert", "save", "update", "delete", "remove", "drop", "createcollection", "createindex", "dropdatabase"};
        for (String keyword : forbidden) {
            if (normalized.contains("." + keyword + "(") || normalized.startsWith(keyword + "(")) {
                throw new IllegalArgumentException("Forbidden Mongo command: " + keyword);
            }
        }
    }

    private Document parseFindFilter(String args) {
        String trimmed = args == null ? "" : args.trim();
        if (trimmed.isEmpty()) {
            return new Document();
        }

        int comma = findTopLevelComma(trimmed);
        String filter = comma >= 0 ? trimmed.substring(0, comma).trim() : trimmed;
        return filter.isEmpty() ? new Document() : Document.parse(filter);
    }

    private List<Document> parsePipeline(String args) {
        BsonArray array = BsonArray.parse(args.trim());
        List<Document> pipeline = new ArrayList<>();
        array.forEach(value -> pipeline.add(Document.parse(value.asDocument().toJson())));
        return pipeline;
    }

    private int findTopLevelComma(String value) {
        int depth = 0;
        boolean inString = false;
        char quote = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (inString) {
                if (c == quote && value.charAt(i - 1) != '\\') {
                    inString = false;
                }
            } else if (c == '"' || c == '\'') {
                inString = true;
                quote = c;
            } else if (c == '{' || c == '[' || c == '(') {
                depth++;
            } else if (c == '}' || c == ']' || c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private List<Map<String, Object>> toRows(Iterable<Document> docs) {
        return toRows(docs, 1000);
    }

    private List<Map<String, Object>> toRows(Iterable<Document> docs, int limit) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Document doc : docs) {
            rows.add(convertDocument(doc));
            if (rows.size() >= limit) {
                break;
            }
        }
        return rows;
    }

    private Map<String, Object> convertDocument(Document doc) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            row.put(entry.getKey(), convertValue(entry.getValue()));
        }
        return row;
    }

    private Object convertValue(Object value) {
        if (value instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        if (value instanceof Document document) {
            return convertDocument(document);
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream().map(this::convertValue).toList();
        }
        return value;
    }

    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String buildSchemaCacheKey(DbConnectionRequest request) {
        return String.join("|",
                nullToEmpty(request.getHost()),
                nullToEmpty(request.getPort()),
                nullToEmpty(request.getDbName()),
                nullToEmpty(request.getSchema()),
                nullToEmpty(request.getUsername())
        );
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private record CachedMetaPack(MetaPackDto metaPack, long createdAtMs) {
    }
}
