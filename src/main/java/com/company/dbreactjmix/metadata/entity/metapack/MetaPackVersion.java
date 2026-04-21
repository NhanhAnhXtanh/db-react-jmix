package com.company.dbreactjmix.metadata.entity.metaset;

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
@Entity
@Table(name = "META_PACK_VERSION", indexes = {
        @Index(name = "IDX_META_PACK_VERSION_PACK", columnList = "META_PACK_ID"),
        @Index(name = "IDX_META_PACK_VERSION_UNIQUE", columnList = "META_PACK_ID, VERSION_NO", unique = true)
})
public class MetaPackVersion {

    @JmixGeneratedValue
    @Column(name = "ID", nullable = false)
    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "META_PACK_ID")
    private MetaPack metaPack;

    @Column(name = "VERSION_NO", nullable = false)
    private Integer versionNo;

    // JSON: { schema: [...all tables+cols...], relations: [...] }
    @Lob
    @Column(name = "FIELD_DATA")
    private String fieldData;

    @Lob
    @Column(name = "HASH_DATA")
    private String hashData;

    @Version
    @Column(name = "VERSION", nullable = false)
    private Integer version;

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

    @DeletedBy
    @Column(name = "DELETED_BY")
    private String deletedBy;

    @DeletedDate
    @Column(name = "DELETED_DATE")
    private OffsetDateTime deletedDate;

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public MetaPack getMetaPack() { return metaPack; }
    public void setMetaPack(MetaPack metaPack) { this.metaPack = metaPack; }

    public Integer getVersionNo() { return versionNo; }
    public void setVersionNo(Integer versionNo) { this.versionNo = versionNo; }

    public String getFieldData() { return fieldData; }
    public void setFieldData(String fieldData) { this.fieldData = fieldData; }

    public String getHashData() { return hashData; }
    public void setHashData(String hashData) { this.hashData = hashData; }

    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public OffsetDateTime getCreatedDate() { return createdDate; }
    public void setCreatedDate(OffsetDateTime createdDate) { this.createdDate = createdDate; }

    public String getLastModifiedBy() { return lastModifiedBy; }
    public void setLastModifiedBy(String lastModifiedBy) { this.lastModifiedBy = lastModifiedBy; }

    public OffsetDateTime getLastModifiedDate() { return lastModifiedDate; }
    public void setLastModifiedDate(OffsetDateTime lastModifiedDate) { this.lastModifiedDate = lastModifiedDate; }

    public String getDeletedBy() { return deletedBy; }
    public void setDeletedBy(String deletedBy) { this.deletedBy = deletedBy; }

    public OffsetDateTime getDeletedDate() { return deletedDate; }
    public void setDeletedDate(OffsetDateTime deletedDate) { this.deletedDate = deletedDate; }
}
