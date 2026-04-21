package com.company.dbreactjmix.metadata.entity.metaset;

import com.company.dbreactjmix.metadata.entity.MetadataConnectionConfig;
import io.jmix.core.annotation.DeletedBy;
import io.jmix.core.annotation.DeletedDate;
import io.jmix.core.entity.annotation.JmixGeneratedValue;
import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.*;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;

import java.time.OffsetDateTime;
import java.util.UUID;

@JmixEntity
@Table(name = "META_SYNC")
@Entity
public class MetaSync {

    @JmixGeneratedValue
    @Column(name = "ID", nullable = false)
    @Id
    private UUID id;

    @JoinColumn(name = "META_SET_ID")
    @ManyToOne(fetch = FetchType.LAZY)
    private MetaSet metaSet;

    @JoinColumn(name = "CONNECTION_CONFIG_ID")
    @ManyToOne(fetch = FetchType.LAZY)
    private MetadataConnectionConfig connectionConfig;

    @Column(name = "FIELD_DATA")
    @Lob
    private String fieldData;

    @Column(name = "HASH_DATA")
    @Lob
    private String hashData;

    @Column(name = "SYNC_VERSION_NO")
    private Integer syncVersionNo;

    @Column(name = "VERSION", nullable = false)
    @Version
    private Integer version;

    @DeletedBy
    @Column(name = "DELETED_BY")
    private String deletedBy;

    @DeletedDate
    @Column(name = "DELETED_DATE")
    private OffsetDateTime deletedDate;

    @CreatedBy
    @Column(name = "CREATED_BY")
    private String createdBy;

    @CreatedDate
    @Column(name = "CREATED_DATE")
    private OffsetDateTime createdDate;

    @LastModifiedBy
    @Column(name = "LAST_MODIFIED_BY")
    private String lastModifiedBy;

    @LastModifiedDate
    @Column(name = "LAST_MODIFIED_DATE")
    private OffsetDateTime lastModifiedDate;

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public MetaSet getMetaSet() { return metaSet; }
    public void setMetaSet(MetaSet metaSet) { this.metaSet = metaSet; }

    public MetadataConnectionConfig getConnectionConfig() { return connectionConfig; }
    public void setConnectionConfig(MetadataConnectionConfig connectionConfig) { this.connectionConfig = connectionConfig; }

    public String getFieldData() { return fieldData; }
    public void setFieldData(String fieldData) { this.fieldData = fieldData; }

    public String getHashData() { return hashData; }
    public void setHashData(String hashData) { this.hashData = hashData; }

    public Integer getSyncVersionNo() { return syncVersionNo; }
    public void setSyncVersionNo(Integer syncVersionNo) { this.syncVersionNo = syncVersionNo; }

    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }

    public String getDeletedBy() { return deletedBy; }
    public void setDeletedBy(String deletedBy) { this.deletedBy = deletedBy; }

    public OffsetDateTime getDeletedDate() { return deletedDate; }
    public void setDeletedDate(OffsetDateTime deletedDate) { this.deletedDate = deletedDate; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public OffsetDateTime getCreatedDate() { return createdDate; }
    public void setCreatedDate(OffsetDateTime createdDate) { this.createdDate = createdDate; }

    public String getLastModifiedBy() { return lastModifiedBy; }
    public void setLastModifiedBy(String lastModifiedBy) { this.lastModifiedBy = lastModifiedBy; }

    public OffsetDateTime getLastModifiedDate() { return lastModifiedDate; }
    public void setLastModifiedDate(OffsetDateTime lastModifiedDate) { this.lastModifiedDate = lastModifiedDate; }
}
