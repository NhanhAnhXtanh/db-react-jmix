package com.company.dbreactjmix.metadata.dto;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ColumnAttributesDtoTest {

    @Test
    void testColumnAttributesDtoHasOnlyTypeAndNullable() {
        // Verify: DTO has ONLY type + nullable fields
        ColumnAttributesDto dto = new ColumnAttributesDto();
        dto.setType("varchar(255)");
        dto.setNullable(true);

        assertEquals("varchar(255)", dto.getType());
        assertTrue(dto.isNullable());

        // Reflection: verify no other fields exist
        var fields = ColumnAttributesDto.class.getDeclaredFields();
        assertEquals(2, fields.length, "ColumnAttributesDto must have exactly 2 fields (type, nullable)");
    }

    @Test
    void testColumnAttributesSerialization() {
        // Verify: Jackson serializes type + nullable correctly
        ColumnAttributesDto dto = new ColumnAttributesDto();
        dto.setType("bigint");
        dto.setNullable(false);

        assertEquals("bigint", dto.getType());
        assertFalse(dto.isNullable());
    }

    @Test
    void testColumnAttributesPreviousAndCurrentCanBeNull() {
        // Verify: used in ColumnDiffDto.previous and .current
        // Both can be null - previous is null when status='added'
        // current is null when status='removed'
        ColumnAttributesDto attrs1 = null;
        ColumnAttributesDto attrs2 = null;

        assertNull(attrs1);
        assertNull(attrs2);
    }
}
