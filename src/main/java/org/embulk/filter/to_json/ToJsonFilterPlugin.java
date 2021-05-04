package org.embulk.filter.to_json;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.util.timestamp.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ToJsonFilterPlugin
        implements FilterPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(ToJsonFilterPlugin.class);
    private static final String DEFAULT_COLUMN_NAME = "json_payload";
    private static final Type DEFAULT_COLUMN_TYPE = Types.STRING;
    private static final ConfigSource DEFAULT_COLUMN_OPTION = Exec.newConfigSource();
    private static final int JSON_COLUMN_INDEX = 0;
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    public interface PluginTask
            extends Task
    {
        @Config("column")
        @ConfigDefault("null")
        Optional<JsonColumn> getJsonColumn();

        @Config("skip_if_null")
        @ConfigDefault("[]")
        List<String> getColumnNamesSkipIfNull();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimezone();

        @Config("default_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        String getDefaultFormat();
    }

    public interface JsonColumn
            extends Task
    {
        @Config("name")
        @ConfigDefault("null")
        Optional<String> getName();

        @Config("type")
        @ConfigDefault("null")
        Optional<Type> getType();
    }

    private ColumnConfig buildJsonColumnConfig(PluginTask task)
    {
        if (!task.getJsonColumn().isPresent()) {
            return newJsonColumnConfig();
        }

        JsonColumn jsonColumn = task.getJsonColumn().get();
        Optional<String> name = jsonColumn.getName();
        Optional<Type> type = jsonColumn.getType();
        return newJsonColumnConfig(name.orElse(DEFAULT_COLUMN_NAME), type.orElse(DEFAULT_COLUMN_TYPE), DEFAULT_COLUMN_OPTION);
    }

    private ColumnConfig newJsonColumnConfig()
    {
        return newJsonColumnConfig(DEFAULT_COLUMN_NAME, DEFAULT_COLUMN_TYPE, DEFAULT_COLUMN_OPTION);
    }

    private ColumnConfig newJsonColumnConfig(String name, Type type, ConfigSource option)
    {
        if (!Types.STRING.equals(type) && !Types.JSON.equals(type)) {
            throw new ConfigException(String.format("Cannot convert JSON to type: %s", type));
        }
        return new ColumnConfig(name, type, option);
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        for (String columnName : task.getColumnNamesSkipIfNull()) {
            logger.debug("Skip a record if `{}` is null", columnName);
        }

        Schema outputSchema = buildOutputSchema(task);
        for (Column column : outputSchema.getColumns()) {
            logger.debug("OutputSchema: {}", column);
        }

        // TODO: Use task.toTaskSource() after dropping v0.9
        control.run(task.dump(), outputSchema);
    }

    private Schema buildOutputSchema(PluginTask task)
    {
        final ColumnConfig jsonColumnConfig = buildJsonColumnConfig(task);

        Column jsonColumn = new Column(JSON_COLUMN_INDEX, jsonColumnConfig.getName(), jsonColumnConfig.getType());
        List<Column> columns = Collections.unmodifiableList(Arrays.asList(jsonColumn));

        return new Schema(columns);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        final String timezone = task.getDefaultTimezone();
        final String format = task.getDefaultFormat();

        TimestampFormatter timestampFormatter = TimestampFormatter.builder(format, true)
                .setDefaultDateFromString("1970-01-01")
                .setDefaultZoneFromString(timezone)
                .build();
        final List<String> columnNamesSkipIfNull = task.getColumnNamesSkipIfNull();

        return new PageOutput()
        {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ColumnVisitorToJsonImpl visitor = new ColumnVisitorToJsonImpl(pageReader, pageBuilder,
                    outputSchema.getColumn(JSON_COLUMN_INDEX), timestampFormatter, columnNamesSkipIfNull);

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    visitor.visit();
                    pageBuilder.addRecord();
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
