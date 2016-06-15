package org.embulk.filter.to_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ColumnVisitorToJsonImpl
        implements ColumnVisitor
{
    private static class ColumnSetter
    {
        private final PageBuilder pageBuilder;
        private final JsonStringSetter jsonStringSetter;
        private final JsonParser jsonParser = new JsonParser();

        interface JsonStringSetter
        {
            void set(String json);

            void set(Column column, String json);

            void set(int index, String json);
        }

        public ColumnSetter(final PageBuilder pageBuilder, final Column outputColumn)
        {
            this.pageBuilder = pageBuilder;

            if (Types.STRING.equals(outputColumn.getType())) {
                this.jsonStringSetter = new JsonStringSetter()
                {
                    @Override
                    public void set(String json)
                    {
                        set(outputColumn, json);
                    }

                    @Override
                    public void set(Column column, String json)
                    {
                        set(column.getIndex(), json);
                    }

                    @Override
                    public void set(int index, String json)
                    {
                        setAsString(index, json);
                    }

                    private void setAsString(int index, String json)
                    {
                        pageBuilder.setString(index, json);
                    }
                };
            }
            else if (Types.JSON.equals(outputColumn.getType())) {
                this.jsonStringSetter = new JsonStringSetter()
                {
                    @Override
                    public void set(String json)
                    {
                        set(outputColumn, json);
                    }

                    @Override
                    public void set(Column column, String json)
                    {
                        set(column.getIndex(), json);
                    }

                    @Override
                    public void set(int index, String json)
                    {
                        setAsJson(index, json);
                    }

                    private void setAsJson(int index, String json)
                    {
                        pageBuilder.setJson(index, jsonParser.parse(json));
                    }
                };
            }
            else {
                throw new ConfigException(String.format("Cannot convert JSON to type: %s", outputColumn.getType()));
            }
        }
    }

    private static final Logger logger = Exec.getLogger(ColumnVisitorToJsonImpl.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Object> map = Maps.newHashMap();
    private final Map<String, Map<String, Object>> mapForOrderedMerge = Maps.newHashMap();
    private final Map<String, Object> mergedMap = Maps.newHashMap();
    private final PageReader pageReader;
    private final ColumnSetter columnSetter;
    private final Column outputColumn;
    private final TimestampFormatter timestampFormatter;
    private List<String> skipColumnsIfNull = Lists.newArrayList();
    private final boolean mergeMode;
    private final List<String> mergePriority;
    private boolean skipRecordFlag = false;

    ColumnVisitorToJsonImpl(PageReader pageReader, PageBuilder pageBuilder,
            Column outputColumn, TimestampFormatter timestampFormatter,
            List<String> skipColumnsIfNull, boolean mergeMode, List<String> mergePriority)
    {
        this.pageReader = pageReader;
        this.columnSetter = new ColumnSetter(pageBuilder, outputColumn);
        this.outputColumn = outputColumn;
        this.timestampFormatter = timestampFormatter;
        this.skipColumnsIfNull = skipColumnsIfNull;
        this.mergeMode = mergeMode;
        this.mergePriority = mergePriority;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setSkipRecordFlagIfNeeded(column);
            putNull(column);
            return;
        }
        map.put(column.getName(), pageReader.getBoolean(column));
    }

    @Override
    public void longColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setSkipRecordFlagIfNeeded(column);
            putNull(column);
            return;
        }
        map.put(column.getName(), pageReader.getLong(column));
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setSkipRecordFlagIfNeeded(column);
            putNull(column);
            return;
        }
        map.put(column.getName(), pageReader.getDouble(column));
    }

    @Override
    public void stringColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setSkipRecordFlagIfNeeded(column);
            putNull(column);
            return;
        }
        map.put(column.getName(), pageReader.getString(column));
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setSkipRecordFlagIfNeeded(column);
            putNull(column);
            return;
        }
        String value = timestampFormatter.format(pageReader.getTimestamp(column));
        map.put(column.getName(), value);
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            setSkipRecordFlagIfNeeded(column);
            if (mergeMode) {
                return;
            }
            else {
                putNull(column);
                return;
            }
        }

        try {
            if (mergeMode && mergePriority.contains(column.getName())) {
                Map<String, Object> parsedJson = objectMapper.readValue(pageReader.getJson(column).toJson(), new TypeReference<Map<String, Object>>(){});
                mapForOrderedMerge.put(column.getName(), parsedJson);
            }
            else {
                map.put(column.getName(), objectMapper.readTree(pageReader.getJson(column).toJson()));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void putNull(Column column)
    {
        map.put(column.getName(), null);
    }

    private void clear()
    {
        skipRecordFlag = false;
        mapForOrderedMerge.clear();
        mergedMap.clear();
        map.clear();
    }

    private void setSkipRecordFlagIfNeeded(Column column)
    {
        if (skipColumnsIfNull.contains(column.getName())) {
            skipRecordFlag = true;
        }
    }

    private boolean shouldSkipRecord()
    {
        return skipRecordFlag;
    }

    public void visit()
    {
        pageReader.getSchema().visitColumns(this);

        try {
            if (!shouldSkipRecord()) {
                if (mergeMode) {
                    mergeMaps();
                    columnSetter.jsonStringSetter.set(buildJsonString(mergedMap));
                }
                else {
                    columnSetter.jsonStringSetter.set(buildJsonString(map));
                }
            }
        }
        finally {
            clear();
        }
    }

    private void mergeMaps()
    {
        mapForOrderedMerge.put(outputColumn.getName(), map);
        for (String columnName : getMergeOrder()) {
            if (mapForOrderedMerge.containsKey(columnName)) {
                for (Map.Entry<String, Object> entry : mapForOrderedMerge.get(columnName).entrySet()) {
                    mergedMap.put(entry.getKey(), entry.getValue());
                    mapForOrderedMerge.remove(columnName);
                }
            }
        }
        if (!mapForOrderedMerge.isEmpty()) {
            String msg = String.format(
                    "Merge Priority does not include all Json columns: left these: %s",
                    Joiner.on(",").join(mapForOrderedMerge.keySet()));
            throw new IllegalStateException(msg);
        }
    }

    private List<String> getMergeOrder()
    {
        List<String> mergeOrder = Lists.reverse(mergePriority);
        if (!mergeOrder.contains(outputColumn.getName())) {
            mergeOrder.add(0, outputColumn.getName());
        }
        return mergeOrder;
    }

    private String buildJsonString(Map hashMap)
    {
        try {
            return objectMapper.writeValueAsString(hashMap);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
