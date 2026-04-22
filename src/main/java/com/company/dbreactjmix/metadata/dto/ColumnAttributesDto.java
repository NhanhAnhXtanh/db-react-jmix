package com.company.dbreactjmix.metadata.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ColumnAttributesDto {
    // Phase 14: type + nullable only. Defer default/length/precision/pk/comment to future phases.
    private String type;        // e.g., "varchar(255)", "bigint", "numeric(10,2)"
    private boolean nullable;   // mapped from MetaSetModelDto.isNull()

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("nullable")
    public boolean isNullable() {
        return nullable;
    }

    @JsonProperty("nullable")
    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }
}
