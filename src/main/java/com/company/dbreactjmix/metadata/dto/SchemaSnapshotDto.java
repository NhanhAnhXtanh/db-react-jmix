package com.company.dbreactjmix.metadata.dto;

import java.util.ArrayList;
import java.util.List;

public class SchemaSnapshotDto {

    private String version;
    private String dataSource;
    private List<MetaSetModelDto> schema = new ArrayList<>();
    private List<RelationItemDto> relations = new ArrayList<>();

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public List<MetaSetModelDto> getSchema() {
        return schema;
    }

    public void setSchema(List<MetaSetModelDto> schema) {
        this.schema = schema;
    }

    public List<RelationItemDto> getRelations() {
        return relations;
    }

    public void setRelations(List<RelationItemDto> relations) {
        this.relations = relations;
    }
}
