package com.company.tachconnectjmix.metadata.entity;

import io.jmix.core.annotation.Secret;
import io.jmix.core.entity.annotation.JmixGeneratedValue;
import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.util.UUID;

@JmixEntity
@Entity
@Table(name = "META_CONNECTION_CONFIG", indexes = {
        @Index(name = "IDX_META_CONNECTION_CONFIG_CODE", columnList = "CODE", unique = true)
})
public class MetadataConnectionConfig {

    @Id
    @Column(name = "ID", nullable = false)
    @JmixGeneratedValue
    private UUID id;

    @Version
    @Column(name = "VERSION", nullable = false)
    private Integer version;

    @Column(name = "CODE", nullable = false, length = 100)
    private String code;

    @Column(name = "DATABASE_TYPE", nullable = false, length = 20)
    private String databaseType;

    @Column(name = "HOST", nullable = false, length = 255)
    private String host;

    @Column(name = "PORT", nullable = false, length = 20)
    private String port;

    @Column(name = "DB_NAME", nullable = false, length = 255)
    private String dbName;

    @Column(name = "USERNAME", nullable = false, length = 255)
    private String username;

    @Secret
    @Column(name = "PASSWORD", nullable = false, length = 255)
    private String password;

    @Column(name = "SCHEMA_NAME", length = 255)
    private String schema;

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

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
