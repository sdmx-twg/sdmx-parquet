# SDMX-Parquet

SDMX-Parquet is the [SDMX](https://sdmx.org) data format based on
[Apache Parquet](https://parquet.apache.org/), the open, column-oriented file
format widely used for data storage and analytics. It is maintained by the
SDMX Technical Working Group (TWG).

## Why SDMX-Parquet

Statistical data disseminated in SDMX is increasingly consumed by data
analytics platforms, data science tools and data lakes rather than by SDMX
applications alone. SDMX-Parquet makes SDMX data directly usable in that
ecosystem:

- **Ready for analytics:** files can be opened by widely used tools such as
  pandas, Polars, R, DuckDB or Apache Spark, without any SDMX-specific
  library.
- **Strongly typed:** each SDMX representation maps to a native Parquet type
  (numbers, timestamps, lists, maps), unlike text formats where every value
  is a string.
- **Efficient:** columnar storage and compression reduce file size, and
  readers can load only the columns and row groups they need, including
  directly from remote storage.
- **Self-describing:** each file carries its schema and identifies its
  dataflow in its metadata.

SDMX-Parquet complements the other SDMX formats: SDMX-ML, SDMX-JSON and
SDMX-CSV remain the formats for full-featured exchange and for human-readable
tabular data.

## Scope

SDMX-Parquet 1.0 covers **data messages**:

- one file contains the complete data of one dataflow, in a flat (one row per
  observation) representation;
- column names are SDMX component IDs, and values are IDs only (no labels);
- time periods are provided both as reported and as start/end timestamps.

Structural metadata, reference metadata, data actions (merge, replace,
delete) and incremental updates are out of scope.

SDMX REST web services can return SDMX-Parquet with the media type
`application/vnd.sdmx.data+parquet; version=1.0.0`, for queries that request
the complete data of a dataflow. See the
[SDMX REST API](https://github.com/sdmx-twg/sdmx-rest) for details.

## Documentation

- [SDMX-Parquet Data Message Field Guide](data-message/sdmx-parquet-field-guide.md):
  file structure, column naming, type mapping, file metadata, relation to
  SDMX REST, and examples.

## Status

SDMX-Parquet 1.0 is **under public review**. Comments are welcome from
everyone: please submit them as
[issues](https://github.com/sdmx-twg/sdmx-parquet/issues) in this repository.
Open points are marked in the field guide.

## Contributing

Feedback and contributions are welcome. Please open an
[issue](https://github.com/sdmx-twg/sdmx-parquet/issues) to report a problem,
ask a question or propose a change.

## Related specifications

- [SDMX-CSV](https://github.com/sdmx-twg/sdmx-csv)
- [SDMX REST API](https://github.com/sdmx-twg/sdmx-rest)
- [Apache Parquet format](https://parquet.apache.org/docs/)
