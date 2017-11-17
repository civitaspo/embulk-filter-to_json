package org.embulk.filter.to_json;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.util.List;

public class ToJsonFilterPlugin
        implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(ToJsonFilterPlugin.class);
    private static final String DEFAULT_COLUMN_NAME = "json_payload";
    private static final Type DEFAULT_COLUMN_TYPE = Types.STRING;
    private static final ConfigSource DEFAULT_COLUMN_OPTION = Exec.newConfigSource();
    private static final int JSON_COLUMN_INDEX = 0;

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
        return newJsonColumnConfig(name.or(DEFAULT_COLUMN_NAME), type.or(DEFAULT_COLUMN_TYPE), DEFAULT_COLUMN_OPTION);
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
        PluginTask task = config.loadConfig(PluginTask.class);

        for (String columnName : task.getColumnNamesSkipIfNull()) {
            logger.debug("Skip a record if `{}` is null", columnName);
        }

        Schema outputSchema = buildOutputSchema(task);
        for (Column column : outputSchema.getColumns()) {
            logger.debug("OutputSchema: {}", column);
        }
        control.run(task.dump(), outputSchema);
    }

    private Schema buildOutputSchema(PluginTask task)
    {
        final ColumnConfig jsonColumnConfig = buildJsonColumnConfig(task);

        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        Column jsonColumn = new Column(JSON_COLUMN_INDEX, jsonColumnConfig.getName(), jsonColumnConfig.getType());
        builder.add(jsonColumn);

        return new Schema(builder.build());
    }

    private static interface FormatterIntlTask extends Task, TimestampFormatter.Task {}
    private static interface FormatterIntlColumnOption extends Task, TimestampFormatter.TimestampColumnOption {}

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final DateTimeZone timezone = DateTimeZone.forID(task.getDefaultTimezone());
        // TODO: Switch to a newer TimestampFormatter constructor after a reasonable interval.
        // Traditional constructor is used here for compatibility.
        final ConfigSource configSource = Exec.newConfigSource();
        configSource.set("format", task.getDefaultFormat());
        configSource.set("timezone", timezone);
        final TimestampFormatter timestampFormatter = new TimestampFormatter(
            Exec.newConfigSource().loadConfig(FormatterIntlTask.class),
            Optional.fromNullable(configSource.loadConfig(FormatterIntlColumnOption.class)));
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
