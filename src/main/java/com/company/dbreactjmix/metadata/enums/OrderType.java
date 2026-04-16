package com.company.dbreactjmix.metadata.enums;

import org.springframework.lang.Nullable;

public enum OrderType {
    ASC,
    DESC;

    @Nullable
    public static OrderType fromId(String id) {
        if (id == null || id.isBlank()) {
            return null;
        }
        for (OrderType type : OrderType.values()) {
            if (type.name().equalsIgnoreCase(id.trim())) {
                return type;
            }
        }
        return null;
    }
}
