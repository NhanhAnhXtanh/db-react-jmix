package com.company.dbreactjmix.metadata.db.service;

import com.company.dbreactjmix.metadata.dto.ColumnAttributesDto;
import com.company.dbreactjmix.metadata.dto.ColumnDiffDto;
import com.company.dbreactjmix.metadata.dto.MetaSetModelDto;
import com.company.dbreactjmix.metadata.dto.SchemaDiffDto;
import com.company.dbreactjmix.metadata.dto.TableDiffDto;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class MetaSchemaDiffService {

    public Map<String, Object> computeDiff(String tableCode, List<MetaSetModelDto> previous, List<MetaSetModelDto> current) {
        Map<String, MetaSetModelDto> previousMap = previous.stream()
                .collect(Collectors.toMap(MetaSetModelDto::getCode, column -> column, (left, right) -> left));
        Map<String, MetaSetModelDto> currentMap = current.stream()
                .collect(Collectors.toMap(MetaSetModelDto::getCode, column -> column, (left, right) -> left));

        List<String> added = currentMap.keySet().stream()
                .filter(key -> !previousMap.containsKey(key))
                .sorted()
                .collect(Collectors.toList());

        List<String> removed = previousMap.keySet().stream()
                .filter(key -> !currentMap.containsKey(key))
                .sorted()
                .collect(Collectors.toList());

        List<Map<String, Object>> changed = currentMap.entrySet().stream()
                .filter(entry -> previousMap.containsKey(entry.getKey()))
                .filter(entry -> isColumnChanged(previousMap.get(entry.getKey()), entry.getValue()))
                .map(entry -> buildChangedColumn(entry.getKey(), previousMap.get(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());

        Map<String, Object> diff = new LinkedHashMap<>();
        diff.put("tableCode", tableCode);
        diff.put("added", added);
        diff.put("removed", removed);
        diff.put("changed", changed);
        return diff;
    }

    public SchemaDiffDto transformToNestedShape(List<Map<String, Object>> flatDiffs) {
        SchemaDiffDto result = new SchemaDiffDto();
        List<TableDiffDto> tables = new ArrayList<>();

        for (Map<String, Object> flatDiff : flatDiffs) {
            TableDiffDto tableDto = new TableDiffDto();
            tableDto.setName((String) flatDiff.get("tableCode"));

            List<ColumnDiffDto> columnDtos = new ArrayList<>();

            if (Boolean.TRUE.equals(flatDiff.get("isNewTable"))) {
                tableDto.setStatus("added");
                appendSimpleColumns(columnDtos, castStringList(flatDiff.get("added")), "added");
            } else if (Boolean.TRUE.equals(flatDiff.get("isDeletedTable"))) {
                tableDto.setStatus("removed");
                appendSimpleColumns(columnDtos, castStringList(flatDiff.get("removed")), "removed");
            } else {
                tableDto.setStatus("modified");
                appendSimpleColumns(columnDtos, castStringList(flatDiff.get("added")), "added");
                appendSimpleColumns(columnDtos, castStringList(flatDiff.get("removed")), "removed");
                appendChangedColumns(columnDtos, castChangedList(flatDiff.get("changed")));
            }

            tableDto.setColumns(columnDtos);
            tables.add(tableDto);
        }

        result.setHasChanges(!tables.isEmpty());
        result.setTables(tables);
        return result;
    }

    private boolean isColumnChanged(MetaSetModelDto previous, MetaSetModelDto current) {
        boolean typeChanged = !Objects.equals(current.getDataType(), previous.getDataType());
        boolean nullableChanged = current.isNull() != previous.isNull();
        return typeChanged || nullableChanged;
    }

    private Map<String, Object> buildChangedColumn(String code, MetaSetModelDto previous, MetaSetModelDto current) {
        Map<String, Object> changed = new LinkedHashMap<>();
        changed.put("code", code);
        changed.put("previous", toAttributes(previous));
        changed.put("current", toAttributes(current));
        return changed;
    }

    private Map<String, Object> toAttributes(MetaSetModelDto column) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("type", column.getDataType());
        attributes.put("nullable", column.isNull());
        return attributes;
    }

    private void appendSimpleColumns(List<ColumnDiffDto> target, List<String> codes, String status) {
        if (codes == null) {
            return;
        }
        for (String code : codes) {
            ColumnDiffDto columnDto = new ColumnDiffDto();
            columnDto.setName(code);
            columnDto.setStatus(status);
            target.add(columnDto);
        }
    }

    private void appendChangedColumns(List<ColumnDiffDto> target, List<Map<String, Object>> changedColumns) {
        if (changedColumns == null) {
            return;
        }
        for (Map<String, Object> changedColumn : changedColumns) {
            ColumnDiffDto columnDto = new ColumnDiffDto();
            columnDto.setName((String) changedColumn.get("code"));
            columnDto.setStatus("modified");
            columnDto.setPrevious(toColumnAttributes(changedColumn.get("previous")));
            columnDto.setCurrent(toColumnAttributes(changedColumn.get("current")));
            target.add(columnDto);
        }
    }

    private ColumnAttributesDto toColumnAttributes(Object rawAttributes) {
        if (!(rawAttributes instanceof Map<?, ?> attributes)) {
            return null;
        }
        ColumnAttributesDto dto = new ColumnAttributesDto();
        dto.setType((String) attributes.get("type"));
        Object nullable = attributes.get("nullable");
        dto.setNullable(nullable instanceof Boolean value ? value : false);
        return dto;
    }

    @SuppressWarnings("unchecked")
    private List<String> castStringList(Object value) {
        return (List<String>) value;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castChangedList(Object value) {
        return (List<Map<String, Object>>) value;
    }
}
