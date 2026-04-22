package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
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

    public MetaPackDto buildMetaPack(DbConnectionRequest request) {
        JsonNode spec = fetchOpenApiSpec(request);
        List<MetaSetModelDto> schemaRows = new ArrayList<>();
        JsonNode paths = spec.path("paths");
        if (paths.isObject()) {
            paths.fields().forEachRemaining(pathEntry -> readPath(pathEntry.getKey(), pathEntry.getValue(), schemaRows));
        }

        MetaPackDto.MetaPackContent content = new MetaPackDto.MetaPackContent();
        content.setVersion("1.0");
        content.setDataSource("restapi");
        content.setSchema(schemaRows);
        content.setRelations(List.of());

        MetaPackDto response = new MetaPackDto();
        response.setMetaPack(content);
        return response;
    }

    private JsonNode fetchOpenApiSpec(DbConnectionRequest request) {
        URI uri = URI.create(resolveSpecUrl(request));
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(20))
                .GET()
                .header("Accept", "application/json, application/yaml;q=0.8, */*;q=0.5");

        if (!isBlank(request.getUsername()) || !isBlank(request.getPassword())) {
            String token = java.util.Base64.getEncoder()
                    .encodeToString(((request.getUsername() == null ? "" : request.getUsername()) + ":" + (request.getPassword() == null ? "" : request.getPassword())).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            builder.header("Authorization", "Basic " + token);
        }

        try {
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalArgumentException("OpenAPI fetch failed: HTTP " + response.statusCode());
            }
            return objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new IllegalArgumentException("OpenAPI response must be JSON. YAML import is not supported yet.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("OpenAPI fetch interrupted", e);
        }
    }

    private String resolveSpecUrl(DbConnectionRequest request) {
        String host = normalizeUrl(request.getHost());
        String schema = request.getSchema();
        if (!isBlank(schema) && isAbsoluteUrl(schema)) {
            return schema;
        }
        if (isBlank(schema)) {
            return host;
        }
        String suffix = schema.startsWith("/") ? schema : "/" + schema;
        return host.endsWith("/") ? host.substring(0, host.length() - 1) + suffix : host + suffix;
    }

    private String normalizeUrl(String value) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("API URL is required");
        }
        return isAbsoluteUrl(value) ? value : "http://" + value;
    }

    private void readPath(String apiPath, JsonNode pathNode, List<MetaSetModelDto> rows) {
        rows.add(buildRow(apiPath, apiPath, null, "endpoint", false, null));

        Iterator<Map.Entry<String, JsonNode>> fields = pathNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String method = entry.getKey().toLowerCase();
            if (!isHttpMethod(method)) {
                continue;
            }

            JsonNode operation = entry.getValue();
            String operationPath = apiPath + "." + method;
            String description = textOrNull(operation.path("summary"));
            if (description == null) {
                description = textOrNull(operation.path("operationId"));
            }
            rows.add(buildRow(method.toUpperCase(), operationPath, apiPath, "operation", false, description));
            readParameters(operationPath, pathNode.path("parameters"), rows);
            readParameters(operationPath, operation.path("parameters"), rows);
            readSchemaBranch(operationPath, operation.path("requestBody"), "request", rows);
            readResponses(operationPath, operation.path("responses"), rows);
        }
    }

    private void readParameters(String parentPath, JsonNode parameters, List<MetaSetModelDto> rows) {
        if (!parameters.isArray()) {
            return;
        }
        for (JsonNode parameter : parameters) {
            String name = textOrNull(parameter.path("name"));
            if (name == null) {
                continue;
            }
            String location = textOrNull(parameter.path("in"));
            String path = parentPath + ".param." + name;
            String type = schemaType(parameter.path("schema"), location == null ? "parameter" : location);
            rows.add(buildRow(name, path, parentPath, type, !parameter.path("required").asBoolean(false), location));
        }
    }

    private void readSchemaBranch(String parentPath, JsonNode container, String code, List<MetaSetModelDto> rows) {
        JsonNode schema = readContentSchema(container.path("content"));
        if (schema.isMissingNode()) {
            return;
        }
        String branchPath = parentPath + "." + code;
        rows.add(buildRow(code, branchPath, parentPath, schemaType(schema, "object"), true, null));
        walkSchema(branchPath, branchPath, schema, rows);
    }

    private void readResponses(String parentPath, JsonNode responses, List<MetaSetModelDto> rows) {
        if (!responses.isObject()) {
            return;
        }
        responses.fields().forEachRemaining(entry -> {
            String status = entry.getKey();
            JsonNode schema = readContentSchema(entry.getValue().path("content"));
            if (schema.isMissingNode()) {
                return;
            }
            String responsePath = parentPath + ".response." + status;
            rows.add(buildRow("response_" + status, responsePath, parentPath, schemaType(schema, "object"), true, textOrNull(entry.getValue().path("description"))));
            walkSchema(responsePath, responsePath, schema, rows);
        });
    }

    private void walkSchema(String parentPath, String pathPrefix, JsonNode schema, List<MetaSetModelDto> rows) {
        JsonNode resolved = schema.path("items").isObject() ? schema.path("items") : schema;
        JsonNode properties = resolved.path("properties");
        if (!properties.isObject()) {
            return;
        }

        properties.fields().forEachRemaining(entry -> {
            String field = entry.getKey();
            JsonNode definition = entry.getValue();
            String path = pathPrefix + "." + field;
            rows.add(buildRow(field, path, parentPath, schemaType(definition, "object"), true, textOrNull(definition.path("description"))));
            if (definition.path("properties").isObject() || definition.path("items").path("properties").isObject()) {
                walkSchema(path, path, definition, rows);
            }
        });
    }

    private JsonNode readContentSchema(JsonNode content) {
        if (!content.isObject()) {
            return MissingNode.getInstance();
        }
        JsonNode jsonSchema = content.path("application/json").path("schema");
        if (!jsonSchema.isMissingNode()) {
            return jsonSchema;
        }
        Iterator<JsonNode> values = content.elements();
        while (values.hasNext()) {
            JsonNode schema = values.next().path("schema");
            if (!schema.isMissingNode()) {
                return schema;
            }
        }
        return MissingNode.getInstance();
    }

    private String schemaType(JsonNode schema, String fallback) {
        String type = textOrNull(schema.path("type"));
        if (type != null) {
            return type;
        }
        String ref = textOrNull(schema.path("$ref"));
        if (ref != null) {
            int slash = ref.lastIndexOf('/');
            return slash >= 0 ? ref.substring(slash + 1) : ref;
        }
        if (schema.path("properties").isObject()) {
            return "object";
        }
        return fallback;
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

    private boolean isHttpMethod(String method) {
        return switch (method) {
            case "get", "post", "put", "patch", "delete", "head", "options", "trace" -> true;
            default -> false;
        };
    }

    private boolean isAbsoluteUrl(String value) {
        return value != null && (value.startsWith("http://") || value.startsWith("https://"));
    }

    private String textOrNull(JsonNode node) {
        return node.isTextual() && !node.asText().isBlank() ? node.asText() : null;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
