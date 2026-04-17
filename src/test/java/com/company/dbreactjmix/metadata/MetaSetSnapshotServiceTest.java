package com.company.dbreactjmix.metadata;

import com.company.dbreactjmix.metadata.db.service.MetaSetSnapshotService;
import com.company.dbreactjmix.metadata.db.service.MetadataJdbcService;
import com.company.dbreactjmix.metadata.dto.DbConnectionRequest;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.RelationItemDto;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSetVersion;
import com.company.dbreactjmix.test_support.AuthenticatedAsAdmin;
import io.jmix.core.DataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@SpringBootTest
@ExtendWith(AuthenticatedAsAdmin.class)
class MetaSetSnapshotServiceTest {

    private String expectedMetaSetCode;

    @Autowired
    private MetaSetSnapshotService metaSetSnapshotService;

    @Autowired
    private DataManager dataManager;

    @MockBean
    private MetadataJdbcService metadataJdbcService;

    @AfterEach
    void tearDown() {
        List<MetaSetVersion> versions = dataManager.load(MetaSetVersion.class).all().list();
        if (!versions.isEmpty()) {
            dataManager.remove(versions);
        }

        List<MetaSet> metaSets = dataManager.load(MetaSet.class).all().list();
        if (!metaSets.isEmpty()) {
            dataManager.remove(metaSets);
        }
    }

    @Test
    void saveSnapshot_createsVersionOnlyWhenHashChanges() {
        DbConnectionRequest request = buildRequest();

        when(metadataJdbcService.buildMetaPack(request))
                .thenReturn(buildMetaPack(List.of(
                        schema("orders.id", "id", true),
                        schema("orders", "orders", false),
                        schema("orders.code", "code", false)
                ), List.of(relation("orders.customer_id->customer.id"))))
                .thenReturn(buildMetaPack(List.of(
                        schema("orders.code", "code", false),
                        schema("orders", "orders", false),
                        schema("orders.id", "id", true)
                ), List.of(relation("orders.customer_id->customer.id"))))
                .thenReturn(buildMetaPack(List.of(
                        schema("orders", "orders", false),
                        schema("orders.id", "id", true),
                        schema("orders.code", "code", false),
                        schema("orders.status", "status", false)
                ), List.of(relation("orders.customer_id->customer.id"))));

        var firstResponse = metaSetSnapshotService.saveSnapshot(request);
        var secondResponse = metaSetSnapshotService.saveSnapshot(request);
        var thirdResponse = metaSetSnapshotService.saveSnapshot(request);

        List<MetaSetVersion> versions = dataManager.load(MetaSetVersion.class)
                .query("e.metaSet.code = :code order by e.versionNo")
                .parameter("code", expectedMetaSetCode)
                .list();
        MetaSet metaSet = dataManager.load(MetaSet.class)
                .query("e.code = :code")
                .parameter("code", expectedMetaSetCode)
                .one();

        assertThat(firstResponse.get("changed")).isEqualTo(true);
        assertThat(firstResponse.get("versionNo")).isEqualTo(1);

        assertThat(secondResponse.get("changed")).isEqualTo(false);
        assertThat(secondResponse.get("versionNo")).isEqualTo(1);

        assertThat(thirdResponse.get("changed")).isEqualTo(true);
        assertThat(thirdResponse.get("versionNo")).isEqualTo(2);

        assertThat(versions).hasSize(2);
        assertThat(versions.get(0).getVersionNo()).isEqualTo(1);
        assertThat(versions.get(1).getVersionNo()).isEqualTo(2);
        assertThat(metaSet.getCurrentVersionNo()).isEqualTo(2);
        assertThat(metaSet.getCurrentHashData()).isEqualTo(thirdResponse.get("hash"));
    }

    private DbConnectionRequest buildRequest() {
        String dbName = "demo_" + System.nanoTime();
        DbConnectionRequest request = new DbConnectionRequest();
        request.setDatabaseType("POSTGRES");
        request.setHost("localhost");
        request.setPort("5432");
        request.setDbName(dbName);
        request.setUsername("demo");
        request.setPassword("secret");
        request.setSchema("public");
        expectedMetaSetCode = "postgres-localhost-5432-" + dbName + "-public";
        return request;
    }

    private MetaPackDto buildMetaPack(List<MetaSetModelDto> schema, List<RelationItemDto> relations) {
        MetaPackDto.MetaPackContent content = new MetaPackDto.MetaPackContent();
        content.setVersion("1.0");
        content.setDataSource("postgres");
        content.setSchema(schema);
        content.setRelations(relations);

        MetaPackDto dto = new MetaPackDto();
        dto.setMetaPack(content);
        return dto;
    }

    private MetaSetModelDto schema(String id, String code, boolean primaryKey) {
        MetaSetModelDto dto = new MetaSetModelDto();
        dto.setId(id);
        dto.setCode(code);
        dto.setName(code);
        dto.setDataType(id.contains(".") ? "varchar" : "collection");
        dto.setPath(id);
        dto.setPath_parent(id.contains(".") ? "orders" : null);
        dto.setDescription(null);
        dto.setNull(!primaryKey);
        dto.setPrimaryKey(primaryKey);
        dto.setComment(null);
        return dto;
    }

    private RelationItemDto relation(String id) {
        RelationItemDto dto = new RelationItemDto();
        dto.setId(id);
        dto.setSourceTable("orders");
        dto.setSourceField("customer_id");
        dto.setTargetTable("customer");
        dto.setTargetField("id");
        dto.setType("N:1");
        return dto;
    }
}
