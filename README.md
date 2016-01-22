# To Json filter plugin for Embulk

Convert a record to jsonl.

## Overview

* **Plugin type**: filter

## Configuration

- **column_name**: json column name (string, default: `"json_payload"`)

## Example

```yaml
filters:
  - type: to_json
    column_name: json_column
```

## Run Example

```
$ ./gradlew classpath
$ embulk run -I lib example/config.yml
```

## TODO
- support json type

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
