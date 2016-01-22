package org.embulk.filter.to_json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.util.Map;

public class ToJsonFilterPlugin
        implements FilterPlugin
{
    private final static Logger logger = Exec.getLogger(ToJsonFilterPlugin.class);

    public interface PluginTask
            extends Task
    {
        @Config("column_name")
        @ConfigDefault("\"json_payload\"")
        public String getColumnName();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = buildOutputSchema(task);
        control.run(task.dump(), outputSchema);
    }

    private Schema buildOutputSchema(PluginTask task)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        Column jsonColumn = new Column(0, task.getColumnName(), Types.STRING);
        builder.add(jsonColumn);
        return new Schema(builder.build());
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new PageOutput() {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    pageBuilder.setString(0, convertRecordToJsonString());
                    pageBuilder.addRecord();
                }
            }

            private String convertRecordToJsonString()
            {
                Map<String, Object> map = Maps.newHashMap();
                for (Column column : inputSchema.getColumns()) {
                    if (pageReader.isNull(column)) {
                        map.put(column.getName(), null);
                        continue;
                    }

                    if (Types.BOOLEAN.equals(column.getType())) {
                        map.put(column.getName(), pageReader.getBoolean(column));
                    }
                    else if (Types.DOUBLE.equals(column.getType())) {
                        map.put(column.getName(), pageReader.getDouble(column));
                    }
                    else if (Types.LONG.equals(column.getType())) {
                        map.put(column.getName(), pageReader.getLong(column));
                    }
                    else if (Types.STRING.equals(column.getType())) {
                        map.put(column.getName(), pageReader.getString(column));
                    }
                    else if (Types.TIMESTAMP.equals(column.getType())) {
                        map.put(column.getName(), pageReader.getTimestamp(column).toString());
                    }
                    else {
                        logger.warn("Unsupported Type `{}`, so put null instead.", column.getType());
                        map.put(column.getName(), null);
                    }
                }

                try {
                    return objectMapper.writeValueAsString(map);
                }
                catch (JsonProcessingException e) {
                    logger.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }
        };
    }
}
