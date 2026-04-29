package com.company.dbreactjmix.metadata.entity;

import io.jmix.core.entity.annotation.JmixGeneratedValue;
import io.jmix.core.metamodel.annotation.InstanceName;
import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.OffsetDateTime;
import java.util.UUID;

@JmixEntity
@Entity
@Table(name = "MONGO_SCHEMA_SNAPSHOT", indexes = {
        @Index(name = "IDX_MONGO_SCHEMA_SNAPSHOT_KEY", columnList = "CONNECTION_KEY", unique = true)
})
public class MongoSchemaSnapshot {

    @Id
    @JmixGeneratedValue
    @Column(name = "ID", nullable = false)
    private UUID id;

    @Version
    @Column(name = "VERSION", nullable = false)
    private Integer version;

    @InstanceName
    @Column(name = "CONNECTION_KEY", nullable = false, length = 512)
    private String connectionKey;

    @Column(name = "STATUS", nullable = false, length = 30)
    private String status;

    @Column(name = "SCHEMA_HASH", length = 64)
    private String schemaHash;

    @Lob
    @Column(name = "SCHEMA_JSON")
    private String schemaJson;

    @Lob
    @Column(name = "CHECKPOINT_JSON")
    private String checkpointJson;

    @Lob
    @Column(name = "RESUME_TOKEN_JSON")
    private String resumeTokenJson;

    @Column(name = "SCANNED_DOCS")
    private Long scannedDocs;

    @Column(name = "TOTAL_COLLECTIONS")
    private Integer totalCollections;

    @Column(name = "PROCESSED_COLLECTIONS")
    private Integer processedCollections;

    @Column(name = "CURRENT_COLLECTION", length = 255)
    private String currentCollection;

    @Column(name = "ERROR_MESSAGE", length = 2000)
    private String errorMessage;

    @Column(name = "STARTED_AT")
    private OffsetDateTime startedAt;

    @Column(name = "COMPLETED_AT")
    private OffsetDateTime completedAt;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getConnectionKey() {
        return connectionKey;
    }

    public void setConnectionKey(String connectionKey) {
        this.connectionKey = connectionKey;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(String schemaHash) {
        this.schemaHash = schemaHash;
    }

    public String getSchemaJson() {
        return schemaJson;
    }

    public void setSchemaJson(String schemaJson) {
        this.schemaJson = schemaJson;
    }

    public String getCheckpointJson() {
        return checkpointJson;
    }

    public void setCheckpointJson(String checkpointJson) {
        this.checkpointJson = checkpointJson;
    }

    public Long getScannedDocs() {
        return scannedDocs;
    }

    public void setScannedDocs(Long scannedDocs) {
        this.scannedDocs = scannedDocs;
    }

    public String getResumeTokenJson() {
        return resumeTokenJson;
    }

    public void setResumeTokenJson(String resumeTokenJson) {
        this.resumeTokenJson = resumeTokenJson;
    }

    public Integer getTotalCollections() {
        return totalCollections;
    }

    public void setTotalCollections(Integer totalCollections) {
        this.totalCollections = totalCollections;
    }

    public Integer getProcessedCollections() {
        return processedCollections;
    }

    public void setProcessedCollections(Integer processedCollections) {
        this.processedCollections = processedCollections;
    }

    public String getCurrentCollection() {
        return currentCollection;
    }

    public void setCurrentCollection(String currentCollection) {
        this.currentCollection = currentCollection;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public OffsetDateTime getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(OffsetDateTime startedAt) {
        this.startedAt = startedAt;
    }

    public OffsetDateTime getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(OffsetDateTime completedAt) {
        this.completedAt = completedAt;
    }
}
