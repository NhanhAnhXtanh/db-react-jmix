package com.company.dbreactjmix.metadata.enums;

import org.springframework.lang.Nullable;

public enum OperationType {
    EQUAL("="),
    GREATER_THAN(">"),
    GREATER_OR_EQUAL(">="),
    LESS_THAN("<"),
    NOT_EQUAL("!="),
    LESS_OR_EQUAL("<="),
    LIKE("LIKE");

    private final String id;

    OperationType(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Nullable
    public static OperationType fromId(String id) {
        if (id == null || id.isBlank()) {
            return null;
        }
        for (OperationType type : OperationType.values()) {
            if (type.id.equalsIgnoreCase(id.trim())) {
                return type;
            }
            if (type.name().equalsIgnoreCase(id.trim())) {
                return type;
            }
        }
        return null;
    }
}
