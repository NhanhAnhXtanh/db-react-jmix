package com.company.dbreactjmix.metadata.dto;

import com.company.dbreactjmix.metadata.enums.OperationType;

public class QueryFilterParam {

    private String key;
    private String operation;
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public OperationType getOperation() {
        return OperationType.fromId(operation);
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
