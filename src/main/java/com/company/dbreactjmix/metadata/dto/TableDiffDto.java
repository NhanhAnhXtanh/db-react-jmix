package com.company.dbreactjmix.metadata.dto;

import java.util.List;

public class TableDiffDto {
    private String name;                    // table name
    private String status;                  // 'added' | 'removed' | 'modified'
    private List<ColumnDiffDto> columns;

    // Getters/setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<ColumnDiffDto> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnDiffDto> columns) {
        this.columns = columns;
    }
}
