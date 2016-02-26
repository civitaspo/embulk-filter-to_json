# To Json filter plugin for Embulk

Convert a record to jsonl.

## Overview

* **Plugin type**: filter

## Configuration

- **column**: output json column (optional)
  - **name** (string, default: `"json_payload"`)
  - **type** string or json (string, default: `"string"`)
- **skip_if_null**: input column name list (array of string, default: `[]`)
- **default_timezone**: option for timestamp column, specify the timezone of timestamp value (string, default is `UTC`)
- **default_format**: option for timestamp column, specify the format of timestamp value (string, default is `%Y-%m-%d %H:%M:%S.%N %z`)

## Example

```yaml
filters:
  - type: to_json
    column:
      name: test
      type: string
    skip_if_null: [id]
    default_timezone: Asia/Tokyo
```

## Run Example

```
$ ./gradlew classpath
$ embulk run -I lib example/config.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
