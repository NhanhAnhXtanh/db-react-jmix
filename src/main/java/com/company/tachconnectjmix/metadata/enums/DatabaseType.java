package com.company.tachconnectjmix.metadata.enums;

import org.springframework.lang.Nullable;

public enum DatabaseType {
    POSTGRES;

    @Nullable
    public static DatabaseType fromId(String id) {
        if (id == null || id.isBlank()) {
            return null;
        }

        for (DatabaseType type : DatabaseType.values()) {
            if (type.name().equalsIgnoreCase(id.trim())) {
                return type;
            }
        }
        return null;
    }
}
