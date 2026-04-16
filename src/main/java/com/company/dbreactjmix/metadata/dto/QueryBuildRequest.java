package com.company.dbreactjmix.metadata.dto;

import java.util.List;

public class QueryBuildRequest {

    private DbConnectionRequest connection;
    private String tableName;
    private List<String> fields;
    private List<QueryFilterParam> filters;
    private List<QueryOrderParam> orders;
    private Integer limit;

    public DbConnectionRequest getConnection() {
        return connection;
    }

    public void setConnection(DbConnectionRequest connection) {
        this.connection = connection;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<QueryFilterParam> getFilters() {
        return filters;
    }

    public void setFilters(List<QueryFilterParam> filters) {
        this.filters = filters;
    }

    public List<QueryOrderParam> getOrders() {
        return orders;
    }

    public void setOrders(List<QueryOrderParam> orders) {
        this.orders = orders;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }
}
