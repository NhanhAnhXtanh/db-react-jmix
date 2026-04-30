package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.SchemaSnapshotDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class ApiMetadataService {

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public ApiMetadataService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public SchemaSnapshotDto buildSchema(DbConnectionRequest request) {
        JsonNode payload = fetchJson(request);
        String rootCode = !isBlank(request.getDbName()) ? request.getDbName() : "api";
        String endpoint = !isBlank(request.getSchema()) ? request.getSchema() : request.getHost();
        List<MetaSetModelDto> schemaRows = new ArrayList<>();

        schemaRows.add(buildRow(rootCode, rootCode, null, "endpoint", false, endpoint));
        JsonNode sample = payload.isArray() && !payload.isEmpty() ? payload.get(0) : payload;
        walkValue(rootCode, rootCode, sample, schemaRows);

        SchemaSnapshotDto response = new SchemaSnapshotDto();
        response.setVersion("1.0");
        response.setDataSource("restapi");
        response.setSchema(schemaRows);
        response.setRelations(List.of());
        return response;
    }

    private JsonNode fetchJson(DbConnectionRequest request) {
        URI uri = URI.create(resolveRequestUrl(request));
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(20))
                .GET()
                .header("Accept", "application/json, */*;q=0.5");

        if (!isBlank(request.getUsername()) || !isBlank(request.getPassword())) {
            String token = java.util.Base64.getEncoder()
                    .encodeToString(((request.getUsername() == null ? "" : request.getUsername()) + ":" + (request.getPassword() == null ? "" : request.getPassword())).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            builder.header("Authorization", "Basic " + token);
        }

        try {
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalArgumentException("API request failed: HTTP " + response.statusCode());
            }
            return objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new IllegalArgumentException("API response must be JSON.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("API request interrupted", e);
        }
    }

    private String resolveRequestUrl(DbConnectionRequest request) {
        String host = normalizeUrl(request.getHost());
        String endpoint = request.getSchema();
        if (!isBlank(endpoint) && isAbsoluteUrl(endpoint)) {
            return endpoint;
        }
        if (isBlank(endpoint)) {
            return host;
        }
        String suffix = endpoint.startsWith("/") ? endpoint : "/" + endpoint;
        return host.endsWith("/") ? host.substring(0, host.length() - 1) + suffix : host + suffix;
    }

    private String normalizeUrl(String value) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("API URL is required");
        }
        return isAbsoluteUrl(value) ? value : "http://" + value;
    }

    private void walkValue(String parentPath, String pathPrefix, JsonNode value, List<MetaSetModelDto> rows) {
        JsonNode sample = value.isArray() && !value.isEmpty() ? value.get(0) : value;
        if (!sample.isObject()) {
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = sample.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String field = entry.getKey();
            JsonNode child = entry.getValue();
            String path = pathPrefix + "." + field;
            rows.add(buildRow(field, path, parentPath, inferType(child), child.isNull(), null));

            JsonNode nested = child.isArray() && !child.isEmpty() ? child.get(0) : child;
            if (nested.isObject()) {
                walkValue(path, path, nested, rows);
            }
        }
    }

    private String inferType(JsonNode node) {
        if (node == null || node.isNull()) return "null";
        if (node.isArray()) return "array";
        if (node.isObject()) return "object";
        if (node.isBoolean()) return "boolean";
        if (node.isIntegralNumber()) return "integer";
        if (node.isFloatingPointNumber() || node.isNumber()) return "number";
        return "string";
    }

    private MetaSetModelDto buildRow(String code, String path, String parentPath, String type, boolean nullable, String description) {
        MetaSetModelDto row = new MetaSetModelDto();
        row.setId(path);
        row.setCode(code);
        row.setName(code);
        row.setDataType(type);
        row.setPath(path);
        row.setPath_parent(parentPath);
        row.setNull(nullable);
        row.setPrimaryKey(false);
        row.setDescription(description);
        return row;
    }

    private boolean isAbsoluteUrl(String value) {
        return value != null && (value.startsWith("http://") || value.startsWith("https://"));
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
