package com.company.dbreactjmix.metadata.query;

import com.company.dbreactjmix.metadata.dto.QueryBuildRequest;
import com.company.dbreactjmix.metadata.dto.QueryFilterParam;
import com.company.dbreactjmix.metadata.dto.QueryOrderParam;
import com.company.dbreactjmix.metadata.enums.OperationType;
import com.company.dbreactjmix.metadata.enums.OrderType;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class SqlBuilderService {

    public String buildSelectSql(QueryBuildRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request is required");
        }

        if (request.getTableName() == null || request.getTableName().isBlank()) {
            throw new IllegalArgumentException("tableName is required");
        }

        String tableName = sanitizeIdentifier(request.getTableName());

        List<String> clauses = new ArrayList<>();
        clauses.add(buildSelectClause(request.getFields()));
        clauses.add("FROM " + tableName);

        String whereClause = buildWhereClause(request.getFilters());
        if (!whereClause.isBlank()) {
            clauses.add(whereClause);
        }

        String orderClause = buildOrderClause(request.getOrders());
        if (!orderClause.isBlank()) {
            clauses.add(orderClause);
        }

        String limitClause = buildLimitClause(request.getLimit());
        if (!limitClause.isBlank()) {
            clauses.add(limitClause);
        }

        return String.join("\n", clauses);
    }

    private String buildSelectClause(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return "SELECT *";
        }

        List<String> sanitized = fields.stream()
                .filter(f -> f != null && !f.isBlank())
                .map(this::sanitizeIdentifier)
                .toList();

        if (sanitized.isEmpty()) {
            return "SELECT *";
        }

        return "SELECT " + String.join(", ", sanitized);
    }

    private String buildWhereClause(List<QueryFilterParam> filters) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }

        List<String> clauses = new ArrayList<>();
        for (QueryFilterParam filter : filters) {
            if (filter == null || filter.getKey() == null || filter.getKey().isBlank()) {
                continue;
            }

            OperationType operation = filter.getOperation();
            if (operation == null) {
                continue;
            }

            String value = filter.getValue();
            if (value == null || value.isBlank()) {
                continue;
            }

            String column = sanitizeIdentifier(filter.getKey());
            String sqlValue = toSqlValue(operation, value);
            clauses.add(column + " " + operation.getId() + " " + sqlValue);
        }

        if (clauses.isEmpty()) {
            return "";
        }
        return "WHERE " + String.join("\n  AND ", clauses);
    }

    private String buildOrderClause(List<QueryOrderParam> orders) {
        if (orders == null || orders.isEmpty()) {
            return "";
        }

        List<String> clauses = orders.stream()
                .filter(o -> o != null && o.getKey() != null && !o.getKey().isBlank())
                .map(o -> {
                    String key = sanitizeIdentifier(o.getKey());
                    OrderType orderType = o.getOrderType() == null ? OrderType.ASC : o.getOrderType();
                    return key + " " + orderType.name();
                })
                .collect(Collectors.toList());

        if (clauses.isEmpty()) {
            return "";
        }
        return "ORDER BY " + String.join(", ", clauses);
    }

    private String buildLimitClause(Integer limit) {
        if (limit == null || limit <= 0) {
            return "";
        }
        return "LIMIT " + limit;
    }

    private String toSqlValue(OperationType operationType, String rawValue) {
        String escaped = rawValue.replace("'", "''");

        if (operationType == OperationType.LIKE) {
            String pattern = escaped;
            if (!escaped.contains("%") && !escaped.contains("_")) {
                pattern = "%" + escaped + "%";
            }
            return "'" + pattern + "'";
        }

        if (isNumeric(rawValue)) {
            return rawValue;
        }

        return "'" + escaped + "'";
    }

    private boolean isNumeric(String value) {
        return value != null && value.matches("-?\\d+(\\.\\d+)?");
    }

    private String sanitizeIdentifier(String identifier) {
        String trimmed = identifier.trim();
        if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_.$]*")) {
            throw new IllegalArgumentException("Invalid identifier: " + identifier);
        }
        return trimmed;
    }
}
