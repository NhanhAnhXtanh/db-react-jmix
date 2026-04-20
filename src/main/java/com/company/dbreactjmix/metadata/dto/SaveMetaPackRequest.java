package com.company.dbreactjmix.metadata.dto;

public class SaveMetaPackRequest {

    private String metaSetCode;
    private String metaSetName;
    private MetaPackDto metaPack;

    public String getMetaSetCode() {
        return metaSetCode;
    }

    public void setMetaSetCode(String metaSetCode) {
        this.metaSetCode = metaSetCode;
    }

    public String getMetaSetName() {
        return metaSetName;
    }

    public void setMetaSetName(String metaSetName) {
        this.metaSetName = metaSetName;
    }

    public MetaPackDto getMetaPack() {
        return metaPack;
    }

    public void setMetaPack(MetaPackDto metaPack) {
        this.metaPack = metaPack;
    }
}
