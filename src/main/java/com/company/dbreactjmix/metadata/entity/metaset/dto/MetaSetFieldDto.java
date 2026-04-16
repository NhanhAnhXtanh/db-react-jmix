package com.company.dbreactjmix.metadata.entity.metaset.dto;

import io.jmix.core.entity.annotation.JmixGeneratedValue;
import io.jmix.core.entity.annotation.JmixId;

public class MetaSetFieldDto {
    @JmixGeneratedValue
    @JmixId
    private String id;

    private String code;

    private String name;

    private String dataType;

    private String path;

    private String path_parent;

    private String description;

    private Boolean isNull;

    private Boolean isPrimaryKey;

    private String comment;

    public void setIsNull(Boolean isNull) {
        this.isNull = isNull;
    }

    public void setIsPrimaryKey(Boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath_parent() {
        return path_parent;
    }

    public void setPath_parent(String path_parent) {
        this.path_parent = path_parent;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getIsNull() {
        return isNull;
    }

    public boolean isNull() {
        return Boolean.TRUE.equals(isNull);
    }

    public void setNull(Boolean aNull) {
        isNull = aNull;
    }

    public Boolean getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public boolean isPrimaryKey() {
        return Boolean.TRUE.equals(isPrimaryKey);
    }

    public void setPrimaryKey(Boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}