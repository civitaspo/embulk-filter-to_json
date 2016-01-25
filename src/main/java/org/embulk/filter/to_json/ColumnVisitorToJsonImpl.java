package org.embulk.filter.to_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Types;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by takahiro.nakayama on 1/23/16.
 */
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

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Object> map;
    private final PageReader pageReader;
    private final ColumnSetter columnSetter;
    private final TimestampFormatter timestampFormatter;
    private List<String> skipColumnsIfNull = Lists.newArrayList();
    private boolean skipRecordFlag = false;

    ColumnVisitorToJsonImpl(PageReader pageReader, PageBuilder pageBuilder,
            Column outputColumn, TimestampFormatter timestampFormatter, List<String> skipColumnsIfNull)
    {
        this.map = Maps.newHashMap();
        this.pageReader = pageReader;
        this.columnSetter = new ColumnSetter(pageBuilder, outputColumn);
        this.timestampFormatter = timestampFormatter;
        this.skipColumnsIfNull = skipColumnsIfNull;
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
            putNull(column);
            return;
        }

        try {
            map.put(column.getName(), objectMapper.readTree(pageReader.getJson(column).toJson()));
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
                columnSetter.jsonStringSetter.set(buildJsonString());
            }
        }
        finally {
            clear();
        }
    }

    private String buildJsonString()
    {
        try {
            return objectMapper.writeValueAsString(map);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
