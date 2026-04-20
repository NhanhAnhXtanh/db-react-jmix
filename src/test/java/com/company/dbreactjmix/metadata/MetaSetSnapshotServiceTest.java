package com.company.dbreactjmix.metadata;

import com.company.dbreactjmix.metadata.db.service.MetaSetSnapshotService;
import com.company.dbreactjmix.metadata.dto.MetaPackDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.RelationItemDto;
import com.company.dbreactjmix.metadata.dto.SaveMetaPackRequest;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSet;
import com.company.dbreactjmix.metadata.entity.metaset.MetaSetVersion;
import com.company.dbreactjmix.test_support.AuthenticatedAsAdmin;
import io.jmix.core.DataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ExtendWith(AuthenticatedAsAdmin.class)
class MetaSetSnapshotServiceTest {

    private String testMetaSetCode;

    @Autowired
    private MetaSetSnapshotService metaSetSnapshotService;

    @Autowired
    private DataManager dataManager;

    @BeforeEach
    void setUp() {
        testMetaSetCode = "test-metaset-" + System.nanoTime();
    }

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
    void saveSnapshot_createsVersionOnlyWhenStructuralHashChanges() {
        // Lần 1: schema gốc
        var first = metaSetSnapshotService.saveSnapshot(buildRequest(
                List.of(
                        schema("orders.id", "id", true),
                        schema("orders", "orders", false),
                        schema("orders.code", "code", false)
                ),
                List.of(relation("orders.customer_id->customer.id"))
        ));

        // Lần 2: cùng dữ liệu, thứ tự khác → hash vẫn giống → không tạo version mới
        var second = metaSetSnapshotService.saveSnapshot(buildRequest(
                List.of(
                        schema("orders.code", "code", false),
                        schema("orders", "orders", false),
                        schema("orders.id", "id", true)
                ),
                List.of(relation("orders.customer_id->customer.id"))
        ));

        // Lần 3: thêm field mới → hash khác → tạo version mới
        var third = metaSetSnapshotService.saveSnapshot(buildRequest(
                List.of(
                        schema("orders", "orders", false),
                        schema("orders.id", "id", true),
                        schema("orders.code", "code", false),
                        schema("orders.status", "status", false)
                ),
                List.of(relation("orders.customer_id->customer.id"))
        ));

        List<MetaSetVersion> versions = dataManager.load(MetaSetVersion.class)
                .query("e.metaSet.code = :code order by e.versionNo")
                .parameter("code", testMetaSetCode)
                .list();
        MetaSet metaSet = dataManager.load(MetaSet.class)
                .query("e.code = :code")
                .parameter("code", testMetaSetCode)
                .one();

        assertThat(first.get("changed")).isEqualTo(true);
        assertThat(first.get("versionNo")).isEqualTo(1);

        assertThat(second.get("changed")).isEqualTo(false);
        assertThat(second.get("versionNo")).isEqualTo(1);

        assertThat(third.get("changed")).isEqualTo(true);
        assertThat(third.get("versionNo")).isEqualTo(2);

        assertThat(versions).hasSize(2);
        assertThat(versions.get(0).getVersionNo()).isEqualTo(1);
        assertThat(versions.get(1).getVersionNo()).isEqualTo(2);
        assertThat(metaSet.getCurrentVersionNo()).isEqualTo(2);
        assertThat(metaSet.getCurrentHashData()).isEqualTo(third.get("hash"));
    }

    @Test
    void saveSnapshot_noNewVersionWhenOnlyDescriptionChanges() {
        // Lần 1: description = null
        var first = metaSetSnapshotService.saveSnapshot(buildRequest(
                List.of(schemaWithDescription("orders.id", "id", true, null)),
                List.of()
        ));

        // Lần 2: description thay đổi → structural hash vẫn giống → không tạo version mới
        var second = metaSetSnapshotService.saveSnapshot(buildRequest(
                List.of(schemaWithDescription("orders.id", "id", true, "Primary key của orders")),
                List.of()
        ));

        assertThat(first.get("changed")).isEqualTo(true);
        assertThat(second.get("changed")).isEqualTo(false);
        assertThat(second.get("versionNo")).isEqualTo(1);
    }

    private SaveMetaPackRequest buildRequest(List<MetaSetModelDto> schemaList, List<RelationItemDto> relations) {
        SaveMetaPackRequest req = new SaveMetaPackRequest();
        req.setMetaSetCode(testMetaSetCode);
        req.setMetaSetName("test.public");
        req.setMetaPack(buildMetaPack(schemaList, relations));
        return req;
    }

    private MetaPackDto buildMetaPack(List<MetaSetModelDto> schemaList, List<RelationItemDto> relations) {
        MetaPackDto.MetaPackContent content = new MetaPackDto.MetaPackContent();
        content.setVersion("1.0");
        content.setDataSource("postgres");
        content.setSchema(schemaList);
        content.setRelations(relations);

        MetaPackDto dto = new MetaPackDto();
        dto.setMetaPack(content);
        return dto;
    }

    private MetaSetModelDto schema(String id, String code, boolean primaryKey) {
        return schemaWithDescription(id, code, primaryKey, null);
    }

    private MetaSetModelDto schemaWithDescription(String id, String code, boolean primaryKey, String description) {
        MetaSetModelDto dto = new MetaSetModelDto();
        dto.setId(id);
        dto.setCode(code);
        dto.setName(code);
        dto.setDataType(id.contains(".") ? "varchar" : "collection");
        dto.setPath(id);
        dto.setPath_parent(id.contains(".") ? "orders" : null);
        dto.setDescription(description);
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
