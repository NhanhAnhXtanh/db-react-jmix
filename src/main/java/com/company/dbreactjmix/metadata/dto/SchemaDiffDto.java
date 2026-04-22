package com.company.dbreactjmix.metadata.dto;

import java.util.List;

public class SchemaDiffDto {
    private boolean hasChanges;
    private List<TableDiffDto> tables;

    // Getters/setters
    public boolean isHasChanges() {
        return hasChanges;
    }

    public void setHasChanges(boolean hasChanges) {
        this.hasChanges = hasChanges;
    }

    public List<TableDiffDto> getTables() {
        return tables;
    }

    public void setTables(List<TableDiffDto> tables) {
        this.tables = tables;
    }
}
