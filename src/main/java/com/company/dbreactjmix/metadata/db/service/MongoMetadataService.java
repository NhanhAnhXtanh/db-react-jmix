package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.SchemaSnapshotDto;
import com.company.dbreactjmix.metadata.entity.MongoSchemaSnapshot;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.jmix.core.DataManager;
import io.jmix.core.security.SystemAuthenticator;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class MongoMetadataService {

    private static final int FAST_SAMPLE_SIZE = 1000;
    private static final int DEEP_SCAN_BATCH_SIZE = 1000;
    private static final int DEFAULT_QUERY_LIMIT = 100;
    private static final int MAX_QUERY_LIMIT = 5000;
    private static final long QUERY_TIMEOUT_MS = 30_000;
    private static final Set<String> FORBIDDEN_AGGREGATE_STAGES = Set.of("$out", "$merge", "$function", "$accumulator");

    private static final Pattern FIND_PATTERN = Pattern.compile(
            "^\\s*db\\.([A-Za-z0-9_\\-]+)\\.find\\s*\\((.*?)\\)\\s*((?:\\.[A-Za-z]+\\s*\\([^)]*\\)\\s*)*)\\s*;?\\s*$",
            Pattern.DOTALL
    );
    private static final Pattern AGGREGATE_PATTERN = Pattern.compile(
            "^\\s*db\\.([A-Za-z0-9_\\-]+)\\.aggregate\\s*\\((.*)\\)\\s*;?\\s*$",
            Pattern.DOTALL
    );
    private static final Pattern CHAIN_CALL_PATTERN = Pattern.compile("\\.([A-Za-z]+)\\s*\\(([^)]*)\\)");
    private static final ConcurrentMap<String, CompletableFuture<Void>> CHANGE_STREAMS = new ConcurrentHashMap<>();

    private final DataManager dataManager;
    private final ObjectMapper objectMapper;
    private final SystemAuthenticator systemAuthenticator;

    public MongoMetadataService(DataManager dataManager, ObjectMapper objectMapper, SystemAuthenticator systemAuthenticator) {
        this.dataManager = dataManager;
        this.objectMapper = objectMapper;
        this.systemAuthenticator = systemAuthenticator;
    }

    public SchemaSnapshotDto buildSchema(DbConnectionRequest request) {
        MongoSchemaSnapshot snapshot = findSnapshot(request).orElse(null);
        if (snapshot != null && !isBlank(snapshot.getSchemaJson())) {
            SchemaSnapshotDto schemaSnapshot = readSchemaSnapshot(snapshot.getSchemaJson());
            if (!"SCANNING".equals(snapshot.getStatus())) {
                runIncrementalScan(request, snapshot, schemaSnapshot);
            }
            if ("FULL".equals(snapshot.getStatus())) {
                startChangeStreamIfSupported(request);
            }
            return schemaSnapshot;
        }

        MongoSchemaScanResult sampled = scanSchema(request, false);
        saveSnapshot(request, sampled.schemaSnapshot(), "PARTIAL", sampled.scannedDocs(), sampled.checkpoints());
        return sampled.schemaSnapshot();
    }

    public Map<String, Object> testConnection(DbConnectionRequest request) {
        try (MongoClient client = createClient(request)) {
            String databaseName = isBlank(request.getDbName()) ? "admin" : request.getDbName();
            MongoDatabase database = client.getDatabase(databaseName);
            Document pingResult = database.runCommand(new Document("ping", 1));

            List<String> collections = new ArrayList<>();
            for (String name : database.listCollectionNames()) {
                collections.add(name);
                if (collections.size() >= 5) {
                    break;
                }
            }

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "ok");
            response.put("database", databaseName);
            response.put("collectionsPreview", collections);
            response.put("ping", pingResult.get("ok"));
            return response;
        } catch (MongoSecurityException e) {
            throw e;
        }
    }

    public Map<String, Object> startDeepScan(DbConnectionRequest request) {
        MongoSchemaSnapshot snapshot = findOrCreateSnapshot(request);
        if ("SCANNING".equals(snapshot.getStatus())) {
            return snapshotStatus(snapshot);
        }

        snapshot.setStatus("SCANNING");
        snapshot.setStartedAt(OffsetDateTime.now());
        snapshot.setCompletedAt(null);
        snapshot.setErrorMessage(null);
        snapshot.setCurrentCollection(null);
        snapshot.setProcessedCollections(0);
        saveEntity(snapshot);

        CompletableFuture.runAsync(() -> {
            try {
                MongoSchemaScanResult full = scanSchema(request, true, this::updateScanProgress, snapshot);
                saveSnapshot(request, full.schemaSnapshot(), "FULL", full.scannedDocs(), full.checkpoints());
                startChangeStreamIfSupported(request);
            } catch (Exception e) {
                try {
                    MongoSchemaSnapshot failed = findOrCreateSnapshot(request);
                    failed.setStatus("FAILED");
                    failed.setCompletedAt(OffsetDateTime.now());
                    failed.setErrorMessage(e.getMessage());
                    failed.setCurrentCollection(null);
                    saveEntity(failed);
                } catch (Exception ignored) {
                    // Best-effort status update
                }
            }
        });

        return snapshotStatus(snapshot);
    }

    public Map<String, Object> getScanStatus(DbConnectionRequest request) {
        return findSnapshot(request)
                .map(this::snapshotStatus)
                .orElseGet(() -> Map.of("status", "NONE"));
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
                MongoFindCommand command = parseFindCommand(findMatcher);
                FindIterable<Document> docs = database.getCollection(command.collection())
                        .find(command.filter())
                        .skip(command.skip())
                        .limit(command.limit())
                        .maxTime(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (command.projection() != null) {
                    docs = docs.projection(command.projection());
                }
                if (command.sort() != null) {
                    docs = docs.sort(command.sort());
                }
                return toRows(docs, command.limit());
            }

            Matcher aggregateMatcher = AGGREGATE_PATTERN.matcher(query);
            if (aggregateMatcher.matches()) {
                String collectionName = aggregateMatcher.group(1);
                List<Document> pipeline = parsePipeline(aggregateMatcher.group(2));
                validateAggregatePipeline(pipeline);
                normalizeAggregateLimit(pipeline);
                AggregateIterable<Document> docs = database.getCollection(collectionName)
                        .aggregate(pipeline)
                        .maxTime(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return toRows(docs, MAX_QUERY_LIMIT);
            }
        }

        throw new IllegalArgumentException("Only read-only find()/aggregate() Mongo queries are supported");
    }

    private MongoSchemaScanResult scanSchema(DbConnectionRequest request, boolean fullScan) {
        return scanSchema(request, fullScan, null, null);
    }

    private MongoSchemaScanResult scanSchema(DbConnectionRequest request, boolean fullScan, ScanProgressListener progressListener, MongoSchemaSnapshot snapshot) {
        try (MongoClient client = createClient(request)) {
            MongoDatabase database = client.getDatabase(request.getDbName());
            Map<String, String> checkpoints = new LinkedHashMap<>();
            List<MetaSetModelDto> schemaRows = new ArrayList<>();
            long scannedDocs = 0L;
            List<String> collections = database.listCollectionNames().into(new ArrayList<>());
            if (snapshot != null) {
                snapshot.setTotalCollections((int) collections.stream().filter(name -> !name.startsWith("system.")).count());
                saveEntity(snapshot);
            }
            int processedCollections = 0;

            for (Document collectionInfo : database.listCollections()) {
                String collectionName = collectionInfo.getString("name");
                if (collectionName == null || collectionName.startsWith("system.")) {
                    continue;
                }

                schemaRows.add(buildCollectionRoot(collectionName));
                Document validator = readValidator(collectionInfo);
                Map<String, MetaSetModelDto> collectionRows = new LinkedHashMap<>();
                convertValidator(collectionName, validator).forEach(row -> collectionRows.put(row.getPath(), row));
                CollectionScanResult scanResult = scanCollectionFields(collectionName, database.getCollection(collectionName), fullScan);
                scanResult.rows().forEach(row -> collectionRows.putIfAbsent(row.getPath(), row));
                scannedDocs += scanResult.scannedDocs();
                if (scanResult.lastObjectIdHex() != null) {
                    checkpoints.put(collectionName, scanResult.lastObjectIdHex());
                }
                schemaRows.addAll(collectionRows.values());
                processedCollections++;
                if (progressListener != null) {
                    progressListener.onProgress(snapshot, collectionName, processedCollections, snapshot != null && snapshot.getTotalCollections() != null ? snapshot.getTotalCollections() : processedCollections, scannedDocs);
                }
            }

            SchemaSnapshotDto response = new SchemaSnapshotDto();
            response.setVersion("1.0");
            response.setDataSource("mongodb");
            response.setSchema(schemaRows);
            response.setRelations(List.of());
            return new MongoSchemaScanResult(response, checkpoints, scannedDocs);
        }
    }

    private void runIncrementalScan(DbConnectionRequest request, MongoSchemaSnapshot snapshot, SchemaSnapshotDto schemaSnapshot) {
        Map<String, String> checkpoints = readCheckpoints(snapshot.getCheckpointJson());
        if (checkpoints.isEmpty()) {
            return;
        }

        Map<String, MetaSetModelDto> mergedRows = schemaMap(schemaSnapshot.getSchema());
        Map<String, String> updatedCheckpoints = new LinkedHashMap<>(checkpoints);
        boolean changed = false;

        try (MongoClient client = createClient(request)) {
            MongoDatabase database = client.getDatabase(request.getDbName());
            for (Map.Entry<String, String> entry : checkpoints.entrySet()) {
                if (!ObjectId.isValid(entry.getValue())) {
                    continue;
                }
                Map<String, MetaSetModelDto> rows = new LinkedHashMap<>();
                ObjectId lastId = new ObjectId(entry.getValue());
                ObjectId maxId = lastId;
                MongoCollection<Document> collection = database.getCollection(entry.getKey());
                for (Document document : collection.find(new Document("_id", new Document("$gt", lastId))).batchSize(DEEP_SCAN_BATCH_SIZE)) {
                    Object id = document.get("_id");
                    if (id instanceof ObjectId objectId && objectId.compareTo(maxId) > 0) {
                        maxId = objectId;
                    }
                    scanDocument(entry.getKey(), document, rows);
                }
                for (MetaSetModelDto row : rows.values()) {
                    if (!mergedRows.containsKey(row.getPath())) {
                        mergedRows.put(row.getPath(), row);
                        changed = true;
                    }
                }
                if (maxId.compareTo(lastId) > 0) {
                    updatedCheckpoints.put(entry.getKey(), maxId.toHexString());
                }
            }
        } catch (Exception ignored) {
            return;
        }

        if (changed) {
            schemaSnapshot.setSchema(new ArrayList<>(mergedRows.values()));
            saveSnapshot(request, schemaSnapshot, snapshot.getStatus(), snapshot.getScannedDocs(), updatedCheckpoints);
        } else if (!updatedCheckpoints.equals(checkpoints)) {
            snapshot.setCheckpointJson(writeJson(updatedCheckpoints));
            saveEntity(snapshot);
        }
    }

    private void startChangeStreamIfSupported(DbConnectionRequest request) {
        String key = buildSchemaCacheKey(request);
        if (CHANGE_STREAMS.containsKey(key)) {
            return;
        }

        CompletableFuture<Void> watcher = CompletableFuture.runAsync(() -> {
            try (MongoClient client = createClient(request)) {
                MongoDatabase database = client.getDatabase(request.getDbName());
                MongoSchemaSnapshot snapshot = findSnapshot(request).orElse(null);
                ChangeStreamIterable<Document> stream = database.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
                if (snapshot != null && !isBlank(snapshot.getResumeTokenJson())) {
                    stream = stream.resumeAfter(BsonDocument.parse(snapshot.getResumeTokenJson()));
                }
                for (ChangeStreamDocument<Document> change : stream) {
                    if (change.getNamespace() == null || change.getFullDocument() == null) {
                        continue;
                    }
                    updateSnapshotWithDocument(request, change.getNamespace().getCollectionName(), change.getFullDocument(), change.getResumeToken());
                }
            } catch (Exception ignored) {
                CHANGE_STREAMS.remove(key);
            }
        });
        CHANGE_STREAMS.put(key, watcher);
    }

    private void updateSnapshotWithDocument(DbConnectionRequest request, String collectionName, Document document, BsonDocument resumeToken) {
        MongoSchemaSnapshot snapshot = findSnapshot(request).orElse(null);
        if (snapshot == null || isBlank(snapshot.getSchemaJson())) {
            return;
        }

        SchemaSnapshotDto schemaSnapshot = readSchemaSnapshot(snapshot.getSchemaJson());
        Map<String, MetaSetModelDto> mergedRows = schemaMap(schemaSnapshot.getSchema());
        Map<String, MetaSetModelDto> documentRows = new LinkedHashMap<>();
        scanDocument(collectionName, document, documentRows);

        boolean changed = false;
        for (MetaSetModelDto row : documentRows.values()) {
            if (!mergedRows.containsKey(row.getPath())) {
                mergedRows.put(row.getPath(), row);
                changed = true;
            }
        }

        Map<String, String> checkpoints = readCheckpoints(snapshot.getCheckpointJson());
        Object id = document.get("_id");
        if (id instanceof ObjectId objectId) {
            checkpoints.put(collectionName, objectId.toHexString());
        }
        if (resumeToken != null) {
            snapshot.setResumeTokenJson(resumeToken.toJson());
        }

        if (changed) {
            schemaSnapshot.setSchema(new ArrayList<>(mergedRows.values()));
            snapshot.setSchemaJson(writeJson(schemaSnapshot));
            snapshot.setSchemaHash(hashSchema(schemaSnapshot.getSchema()));
        }
        snapshot.setCheckpointJson(writeJson(checkpoints));
        saveEntity(snapshot);
    }

    private CollectionScanResult scanCollectionFields(
            String collectionName,
            MongoCollection<Document> collection,
            boolean fullScan
    ) {
        Map<String, MetaSetModelDto> rows = new LinkedHashMap<>();
        ObjectId maxObjectId = null;
        long scannedDocs = 0L;
        Iterable<Document> documents = fullScan
                ? collection.find().batchSize(DEEP_SCAN_BATCH_SIZE)
                : sampleDocuments(collection);

        for (Document document : documents) {
            scannedDocs++;
            Object id = document.get("_id");
            if (id instanceof ObjectId objectId && (maxObjectId == null || objectId.compareTo(maxObjectId) > 0)) {
                maxObjectId = objectId;
            }
            scanDocument(collectionName, document, rows);
        }
        return new CollectionScanResult(new ArrayList<>(rows.values()), maxObjectId == null ? null : maxObjectId.toHexString(), scannedDocs);
    }

    private Iterable<Document> sampleDocuments(MongoCollection<Document> collection) {
        return collection.find()
                .sort(new Document("_id", -1))
                .limit(FAST_SAMPLE_SIZE)
                .maxTime(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void scanDocument(String collectionName, Document document, Map<String, MetaSetModelDto> rows) {
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            scanValue(collectionName, collectionName, entry.getKey(), entry.getValue(), rows);
        }
    }

    private void scanValue(String parentPath, String pathPrefix, String field, Object value, Map<String, MetaSetModelDto> rows) {
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

    private MongoFindCommand parseFindCommand(Matcher matcher) {
        String collectionName = matcher.group(1);
        List<String> args = splitTopLevelArguments(matcher.group(2));
        Document filter = args.isEmpty() || args.get(0).isBlank() ? new Document() : Document.parse(args.get(0));
        Document projection = args.size() < 2 || args.get(1).isBlank() ? null : Document.parse(args.get(1));

        Document sort = null;
        int skip = 0;
        Integer requestedLimit = null;
        Matcher chainMatcher = CHAIN_CALL_PATTERN.matcher(matcher.group(3));
        while (chainMatcher.find()) {
            String method = chainMatcher.group(1).toLowerCase();
            String value = chainMatcher.group(2).trim();
            switch (method) {
                case "sort" -> sort = Document.parse(value);
                case "skip" -> skip = Math.max(0, Integer.parseInt(value));
                case "limit" -> requestedLimit = Integer.parseInt(value);
                default -> throw new IllegalArgumentException("Unsupported Mongo find chain method: " + method);
            }
        }

        int limit = requestedLimit == null ? DEFAULT_QUERY_LIMIT : Math.max(0, Math.min(requestedLimit, MAX_QUERY_LIMIT));
        return new MongoFindCommand(collectionName, filter, projection, sort, skip, limit);
    }

    private List<String> splitTopLevelArguments(String value) {
        List<String> result = new ArrayList<>();
        String text = value == null ? "" : value.trim();
        if (text.isEmpty()) {
            return result;
        }

        int depth = 0;
        int start = 0;
        boolean inString = false;
        char quote = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inString) {
                if (c == quote && (i == 0 || text.charAt(i - 1) != '\\')) {
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
                result.add(text.substring(start, i).trim());
                start = i + 1;
            }
        }
        result.add(text.substring(start).trim());
        return result;
    }

    private List<Document> parsePipeline(String args) {
        BsonArray array = BsonArray.parse(args.trim());
        List<Document> pipeline = new ArrayList<>();
        array.forEach(value -> pipeline.add(Document.parse(value.asDocument().toJson())));
        return pipeline;
    }

    private void validateAggregatePipeline(List<Document> pipeline) {
        for (Document stage : pipeline) {
            for (String key : stage.keySet()) {
                if (FORBIDDEN_AGGREGATE_STAGES.contains(key)) {
                    throw new IllegalArgumentException("Forbidden aggregate stage: " + key);
                }
            }
        }
    }

    private void normalizeAggregateLimit(List<Document> pipeline) {
        for (Document stage : pipeline) {
            Object limit = stage.get("$limit");
            if (limit instanceof Number number && number.intValue() > MAX_QUERY_LIMIT) {
                stage.put("$limit", MAX_QUERY_LIMIT);
                return;
            }
            if (limit instanceof Number) {
                return;
            }
        }
        pipeline.add(new Document("$limit", DEFAULT_QUERY_LIMIT));
    }

    private void validateReadOnly(String query) {
        String normalized = query.strip().replaceAll("\\s+", " ").toLowerCase();
        String[] forbidden = {"insert", "save", "update", "delete", "remove", "drop", "createcollection", "createindex", "dropdatabase", "runcommand", "bulkwrite"};
        for (String keyword : forbidden) {
            if (normalized.contains("." + keyword + "(") || normalized.startsWith(keyword + "(")) {
                throw new IllegalArgumentException("Forbidden Mongo command: " + keyword);
            }
        }
    }

    private void saveSnapshot(DbConnectionRequest request, SchemaSnapshotDto schemaSnapshot, String status, Long scannedDocs, Map<String, String> checkpointsOverride) {
        MongoSchemaSnapshot snapshot = findOrCreateSnapshot(request);
        Map<String, String> checkpoints = checkpointsOverride != null ? checkpointsOverride : new LinkedHashMap<>();
        snapshot.setStatus(status);
        snapshot.setSchemaJson(writeJson(schemaSnapshot));
        snapshot.setSchemaHash(hashSchema(schemaSnapshot.getSchema()));
        snapshot.setCheckpointJson(writeJson(checkpoints));
        snapshot.setScannedDocs(scannedDocs);
        snapshot.setCompletedAt(OffsetDateTime.now());
        snapshot.setCurrentCollection(null);
        snapshot.setProcessedCollections(snapshot.getTotalCollections());
        snapshot.setErrorMessage(null);
        saveEntity(snapshot);
    }

    private Optional<MongoSchemaSnapshot> findSnapshot(DbConnectionRequest request) {
        return systemAuthenticator.withSystem(() ->
                dataManager.load(MongoSchemaSnapshot.class)
                        .query("e.connectionKey = :key")
                        .parameter("key", buildSchemaCacheKey(request))
                        .optional()
        );
    }

    private MongoSchemaSnapshot findOrCreateSnapshot(DbConnectionRequest request) {
        return findSnapshot(request).orElseGet(() -> {
            MongoSchemaSnapshot snapshot = systemAuthenticator.withSystem(() -> dataManager.create(MongoSchemaSnapshot.class));
            snapshot.setConnectionKey(buildSchemaCacheKey(request));
            snapshot.setStatus("NONE");
            snapshot.setScannedDocs(0L);
            return snapshot;
        });
    }

    private Map<String, Object> snapshotStatus(MongoSchemaSnapshot snapshot) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", snapshot.getStatus());
        result.put("schemaHash", snapshot.getSchemaHash());
        result.put("scannedDocs", snapshot.getScannedDocs());
        result.put("totalCollections", snapshot.getTotalCollections());
        result.put("processedCollections", snapshot.getProcessedCollections());
        result.put("currentCollection", snapshot.getCurrentCollection());
        result.put("startedAt", snapshot.getStartedAt());
        result.put("completedAt", snapshot.getCompletedAt());
        result.put("errorMessage", snapshot.getErrorMessage());
        return result;
    }

    private MongoClient createClient(DbConnectionRequest request) {
        return MongoClients.create(new ConnectionString(buildConnectionString(request)));
    }

    private String buildConnectionString(DbConnectionRequest request) {
        if (isBlank(request.getHost())) {
            throw new IllegalArgumentException("Host hoặc Mongo URI không được để trống");
        }
        if (isMongoUri(request.getHost())) {
            return appendMongoOptions(request.getHost(), request);
        }

        String userInfo = "";
        if (!isBlank(request.getUsername())) {
            userInfo = encode(request.getUsername()) + ":" + encode(request.getPassword() == null ? "" : request.getPassword()) + "@";
        }

        return appendMongoOptions("mongodb://" + userInfo + request.getHost() + ":" + request.getPort() + "/" + request.getDbName(), request);
    }

    private String appendMongoOptions(String uri, DbConnectionRequest request) {
        boolean isSrv = uri.startsWith("mongodb+srv://");
        List<String> options = new ArrayList<>();
        if (!isBlank(request.getSchema()) && !uri.contains("authSource=")) {
            options.add("authSource=" + encode(request.getSchema()));
        }
        if (!uri.contains("serverSelectionTimeoutMS=")) options.add("serverSelectionTimeoutMS=5000");
        if (!isSrv && !uri.contains("connectTimeoutMS=")) options.add("connectTimeoutMS=5000");
        if (!isSrv && !uri.contains("socketTimeoutMS=")) options.add("socketTimeoutMS=30000");

        if (options.isEmpty()) {
            return uri;
        }
        return uri + (uri.contains("?") ? "&" : "?") + String.join("&", options);
    }

    private boolean isMongoUri(String value) {
        return value != null && (value.startsWith("mongodb://") || value.startsWith("mongodb+srv://"));
    }

    private Document readValidator(Document collectionInfo) {
        Document options = collectionInfo.get("options", Document.class);
        return options == null ? null : options.get("validator", Document.class);
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

    private void walkProperties(String parentPath, String pathPrefix, Document properties, Set<String> required, List<MetaSetModelDto> rows) {
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
                walkProperties(path, path, definition.get("properties", Document.class), new HashSet<>(readRequired(definition)), rows);
            } else if ("array".equals(type)) {
                Document items = definition.get("items", Document.class);
                if (items != null && "object".equals(items.getString("bsonType"))) {
                    walkProperties(path, path, items.get("properties", Document.class), new HashSet<>(readRequired(items)), rows);
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
        if (value instanceof ObjectId) return "objectId";
        if (value instanceof Document) return "object";
        if (value instanceof Collection<?>) return "array";
        if (value instanceof Integer || value instanceof Long || value instanceof Short) return "integer";
        if (value instanceof Number) return "number";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof Date) return "date";
        if (value == null) return "null";
        return "string";
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
        if (value instanceof ObjectId objectId) return objectId.toHexString();
        if (value instanceof Document document) return convertDocument(document);
        if (value instanceof Collection<?> collection) return collection.stream().map(this::convertValue).toList();
        return value;
    }

    private Map<String, MetaSetModelDto> schemaMap(List<MetaSetModelDto> schema) {
        Map<String, MetaSetModelDto> rows = new LinkedHashMap<>();
        for (MetaSetModelDto row : schema) {
            rows.put(row.getPath(), row);
        }
        return rows;
    }

    private String hashSchema(List<MetaSetModelDto> rows) {
        String canonical = rows.stream()
                .sorted(Comparator.comparing(MetaSetModelDto::getPath))
                .map(row -> row.getPath() + ":" + row.getDataType() + ":" + row.isNull() + ":" + row.isPrimaryKey())
                .reduce("", (left, right) -> left + "\n" + right);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : bytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            return Base64.getEncoder().encodeToString(canonical.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize Mongo schema snapshot", e);
        }
    }

    private MongoSchemaSnapshot saveEntity(MongoSchemaSnapshot snapshot) {
        return systemAuthenticator.withSystem(() -> dataManager.save(snapshot));
    }

    private SchemaSnapshotDto readSchemaSnapshot(String json) {
        try {
            return objectMapper.readValue(json, SchemaSnapshotDto.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot read Mongo schema snapshot", e);
        }
    }

    private Map<String, String> readCheckpoints(String json) {
        if (isBlank(json)) {
            return new HashMap<>();
        }
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            return new HashMap<>();
        }
    }

    private String buildSchemaCacheKey(DbConnectionRequest request) {
        return String.join("|",
                nullToEmpty(request.getHost()),
                nullToEmpty(request.getPort()),
                nullToEmpty(request.getDbName()),
                nullToEmpty(request.getSchema()),
                nullToEmpty(request.getUsername()),
                nullToEmpty(request.getPassword())
        );
    }

    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private void updateScanProgress(MongoSchemaSnapshot snapshot, String collectionName, int processedCollections, int totalCollections, long scannedDocs) {
        if (snapshot == null) {
            return;
        }
        try {
            MongoSchemaSnapshot fresh = systemAuthenticator.withSystem(() ->
                    dataManager.load(MongoSchemaSnapshot.class)
                            .id(snapshot.getId())
                            .optional()
                            .orElse(snapshot)
            );
            fresh.setCurrentCollection(collectionName);
            fresh.setProcessedCollections(processedCollections);
            fresh.setTotalCollections(totalCollections);
            fresh.setScannedDocs(scannedDocs);
            saveEntity(fresh);
        } catch (Exception ignored) {
            // Progress update is non-critical — skip on concurrent write conflict
        }
    }

    private record MongoFindCommand(
            String collection,
            Document filter,
            Document projection,
            Document sort,
            int skip,
            int limit
    ) {
    }

    private record CollectionScanResult(List<MetaSetModelDto> rows, String lastObjectIdHex, long scannedDocs) {
    }

    private record MongoSchemaScanResult(SchemaSnapshotDto schemaSnapshot, Map<String, String> checkpoints, long scannedDocs) {
    }

    @FunctionalInterface
    private interface ScanProgressListener {
        void onProgress(MongoSchemaSnapshot snapshot, String collectionName, int processedCollections, int totalCollections, long scannedDocs);
    }
}
