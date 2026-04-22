package com.company.dbreactjmix.metadata.dto;

public class ColumnDiffDto {
    private String name;                        // column name
    private String status;                      // 'added' | 'removed' | 'modified'
    private ColumnAttributesDto previous;       // null if status='added'
    private ColumnAttributesDto current;        // null if status='removed'

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

    public ColumnAttributesDto getPrevious() {
        return previous;
    }

    public void setPrevious(ColumnAttributesDto previous) {
        this.previous = previous;
    }

    public ColumnAttributesDto getCurrent() {
        return current;
    }

    public void setCurrent(ColumnAttributesDto current) {
        this.current = current;
    }
}
