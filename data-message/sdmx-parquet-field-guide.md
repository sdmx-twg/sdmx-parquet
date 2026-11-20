# SDMX-Parquet Data Message Field Guide

## Introduction

SDMX-Parquet Data Message is an SDMX data exchange format based on
[Apache Parquet](https://parquet.apache.org/docs/), registered with IANA as
the media type
[`application/vnd.apache.parquet`](https://www.iana.org/assignments/media-types/application/vnd.apache.parquet).
Parquet is an efficient, general-purpose, column-oriented file format used
across a wide variety of platforms, technologies and environments.

SDMX-Parquet is designed for data analytics use cases. It is a tabular
("SDMX flat") representation, optimised for storage and query performance,
that can be read directly by widely used analytics tools. It complements
SDMX-CSV, which remains the human-readable tabular format.

SDMX-Parquet 1.0 is defined for data messages only. It is not defined for
structural metadata or reference metadata.

### SDMX versions

SDMX-Parquet 1.0 applies to both **SDMX 2.1** and **SDMX 3.x**:

- it can represent data defined with SDMX 2.1 or SDMX 3.x structures;
- it can be returned by SDMX REST web services (see
  [Relation to SDMX REST](#relation-to-sdmx-rest)).

Some features only exist in SDMX 3.x data: multiple measures, multi-valued
and multi-lingual attributes, and the GeospatialInformation representation.
Their mappings apply only when such components are present.

An SDMX-Parquet file can also be registered in an SDMX registry as the data
source of a data registration (SDMX 3.2), as a file whose format is the
SDMX-Parquet media type, e.g. `application/vnd.sdmx.data+parquet;version=1.0.0`.

### Conventions

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY" and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119)
and [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174) when, and only when,
they appear in all capitals.

This guide distinguishes two roles:

- **Sender**: a system that writes an SDMX-Parquet file, e.g. an SDMX web
  service or a data provider.
- **Receiver**: a system that reads an SDMX-Parquet file, e.g. an analytics
  tool or an SDMX storage system.

### Parquet in brief

Implementers should read the
[Parquet documentation](https://parquet.apache.org/docs/). The concepts used
in this guide are:

- **Schema:** every Parquet file carries its own schema, stored in the file
  footer. Each column has a **physical type** (how values are stored, e.g.
  `INT32`, `BYTE_ARRAY`) and optionally a **logical type** (how values are
  interpreted, e.g. `STRING`, `TIMESTAMP`, `LIST`, `MAP`).
- **Nulls:** columns declared `optional` can contain null values, stored
  efficiently without placeholders.
- **Row groups:** data is written in horizontal chunks, each storing its
  columns contiguously, with statistics (min/max) that let readers skip data.
- **Key-value metadata:** the file footer can hold string key/value pairs
  describing the file.
- **Encodings and compression:** e.g. dictionary encoding of repetitive
  values, and Snappy or Zstandard compression, are transparent to readers.

Writing is streamable: a sender can produce and send row groups as data is
read from its source, holding only the current row group in memory. Reading
is not: the schema and the row group index are written in the footer at the
end of the file, so a receiver needs the end of the file (the complete file,
or random access such as HTTP range requests) before it can read any data.

## Message overview

An SDMX-Parquet data message is a Parquet file containing data of **one
structure**: a dataflow, a provision agreement or a data structure definition.
Each row contains the information related to **one
observation**: its dimension values, measure values and attribute values.
Values of attributes attached to a partial key (series, group or dataset
level) are repeated on each related observation row.

- The data MAY be delivered as one file or split across several files, e.g.
  partitioned by a dimension or by provision agreement (see
  [Partitioning](#partitioning)).
  Each file is an SDMX-Parquet file on its own: it has all the component
  columns and the file metadata defined in this guide.
- Attribute values attached to a partial key for which no observation exists
  cannot be represented, as there is no row to hold them.

### Column overview

| Column name | Description | Required |
| --- | --- | --- |
| **Component columns** | | |
| [`COMPONENT_ID`](#component-columns), e.g. `REF_AREA`, `OBS_VALUE` | One column per dimension, measure and attribute of the data structure definition, named by its component ID. For multi-column types, holds the value as reported. | Yes, for all components |
| **Companion columns** (multi-column types) | | |
| [`_COMPONENT_ID_begin`](#time-period-columns) | Start of the reported time period (inclusive). | Yes, for time period types |
| [`_COMPONENT_ID_end`](#time-period-columns) | End of the reported time period (inclusive). | Yes, for time period types |
| [`_COMPONENT_ID_geo`](#geospatial-columns) | Geometry as WKB. | Yes, for GeospatialInformation |
| [`_COMPONENT_ID_X`](#geospatial-columns) | Longitude of Point and PointZ geometries. | Yes, for GeospatialInformation |
| [`_COMPONENT_ID_Y`](#geospatial-columns) | Latitude of Point and PointZ geometries. | Yes, for GeospatialInformation |
| [`_COMPONENT_ID_Z`](#geospatial-columns) | Altitude of PointZ geometries. | Yes, for GeospatialInformation |
| **Other columns** | | |
| [Custom column](#custom-columns) | Any implementer-defined column. | No |

There are no columns for the structure identification or the action: the
structure is identified in the [file metadata](#file-metadata), and
[data actions](#data-actions-and-incremental-updates) are not supported.
Labels are not included (IDs only).

## Design principles

- **One structure per file:** the file contains data of a single dataflow,
  provision agreement or data structure definition, as in SDMX-CSV.
- **Sender-defined content:** a file typically contains the complete data of
  the structure, but the data can also be split across several files (see
  [Partitioning](#partitioning)), or be a subset returned by a web service
  that supports filtering (see [Relation to SDMX REST](#relation-to-sdmx-rest)).
- **Flat representation:** one row per observation, with all components as
  columns.
- **Cube components:** the combination of dimension values can be expressed
  as cube components.
- **Strong typing:** every SDMX representation maps to a specific Parquet
  physical and logical type (see [Type mapping](#type-mapping)), unlike
  SDMX-CSV, where every value is a string.
- **Native multiple values:** multi-valued components use the Parquet `LIST`
  type, not delimited strings.
- **IDs only:** coded values are their code IDs; localised names are not
  included.
- **Readable by non-SDMX software:** a file can readily be opened for analysis
  by software that is not SDMX-specific (see [Reading examples](#reading-examples)).

## File metadata

SDMX-specific information about the file is stored in the **file-level
key-value metadata** of the Parquet footer (`FileMetaData.key_value_metadata`
in the Parquet format).

| Key | Required | Content | Example |
| --- | --- | --- | --- |
| `sdmx.structure` | Yes | Type of structure, as in SDMX-CSV `STRUCTURE`: `dataflow`, `dataprovision` (provision agreement) or `datastructure` (data structure definition). | `dataflow` |
| `sdmx.structure_id` | Yes | Identification `AGENCY:ID(VERSION)` of the dataflow, provision agreement or data structure definition, as in SDMX-CSV `STRUCTURE_ID`. For non-versioned artefacts the version is omitted, e.g. `AGENCY:DF_ID`. | `ESTAT:NA_MAIN(1.6.0)` |
| `sdmx.format_version` | Yes | Version of the SDMX-Parquet specification. | `1.0.0` |
| `sdmx.sender` | Recommended | ID of the organisation that produced the file, as the `Sender` field of an SDMX message header. | `ESTAT` |
| `sdmx.as_of` | Recommended | The point in time the data reflect, as an `xs:dateTime` in UTC, e.g. when the data were extracted, or the point in time requested by an `asOf` query. | `2026-10-01T00:00:00Z` |

- Keys starting with `sdmx.` are reserved for this specification.
- Senders MAY add other keys. Keys defined by other conventions are
  unaffected, e.g. `geo` for [GeoParquet](#geospatial-columns).
- `sdmx.structure_id` MUST identify a single, concrete version: wildcards and the version operators of SDMX REST queries
  (`*`, `+`, `~`) are not allowed.
- A file without the required keys is not an SDMX-Parquet file.
- Receivers MUST reject a file whose `sdmx.structure` value is not one of
  `dataflow`, `dataprovision` or `datastructure`, or whose
  `sdmx.format_version` has a major version they do not
  support. Receivers MAY ignore keys they do not know.

## Columns

### Component columns

- Each column is named by the ID of its SDMX component.
- Unless a web service returns only the components requested by a query (see
  [Relation to SDMX REST](#relation-to-sdmx-rest)), the file MUST include a
  column for every component defined by the data structure definition:
  dimensions, measures and attributes. A measure or attribute column with no
  values for any row contains only nulls.
- How attributes apply to a constrained dataflow (e.g. an attribute related
  only to a dimension that the dataflow constraint excludes) is being clarified
  in [sdmx-im#75](https://github.com/sdmx-twg/sdmx-im/issues/75). Until then,
  the file includes a column for every attribute of the data structure
  definition.
- For a dataflow or a provision agreement based on an evolving data structure
  (SDMX 3.x), the dimensions are those fixed by the dataflow's dimension
  constraint.
- Metadata attributes, defined in a metadata structure definition linked to
  the data structure definition, MAY be included as columns. SDMX-Parquet 1.0
  gives no guidelines for their columns (e.g. for hierarchical metadata
  attributes); receivers MAY handle them as [custom columns](#custom-columns).
- Each column MUST have the Parquet type defined in
  [Type mapping](#type-mapping) for the component's representation.
- Senders SHOULD order component columns as defined in the data structure
  definition, grouped by type of component: dimensions, then measures, then
  attributes.
- Receivers MUST accept columns in any order and MUST identify them by name.

### Companion columns

Components whose type maps to "multi column" in [Type mapping](#type-mapping)
are represented by several columns:

- the component column, named by the component ID, holding the value **as
  reported**, e.g. `TIME_PERIOD` = `2020-Q1`;
- companion columns, named `_{COMPONENT_ID}_{reserved name}`, e.g.
  `_TIME_PERIOD_begin`.

Companion columns MUST be present for every multi-column component. The
reserved names are defined in [Time period columns](#time-period-columns) and
[Geospatial columns](#geospatial-columns).

### Custom columns

- Senders MAY add other columns. Their presence does not invalidate an
  SDMX-Parquet file.
- Custom column names MUST NOT be equal to a component ID of the data
  structure definition, and MUST NOT follow the companion column pattern
  `_{COMPONENT_ID}_{reserved name}`.
- Receivers MAY ignore custom columns.

## Type mapping

### General rules

- **Coded components** (with a codelist or valuelist enumeration) map to
  `STRING` (`BYTE_ARRAY`), holding the code ID as a simple string.
- **Multi-valued components** map to a Parquet `LIST` whose element type is
  the mapped type of a single value, e.g. `LIST<STRING>`. Values are kept in
  their reported order.
- **Multi-lingual components** map to a Parquet `MAP`, see
  [Multi-lingual components](#multi-lingual-components).
- Nullability depends on the type of component, see
  [Nullability](#nullability).
- `TIMESTAMP` values MUST use the unit `MILLIS`. Fractions of a second beyond
  milliseconds MUST be truncated.
- **DateTime** components map to `TIMESTAMP(MILLIS, isAdjustedToUTC=true)`,
  i.e. UTC instants:
    - values with a time zone MUST be converted to UTC;
    - values without a time zone, which `xs:dateTime` allows (see
      [SDMXCommon.xsd](https://github.com/sdmx-twg/sdmx-ml/blob/master/schemas/SDMXCommon.xsd#L1409-L1412)),
      MUST be taken as UTC.
- **Time period companion columns** (`_begin`, `_end`) map to
  `TIMESTAMP(MILLIS, isAdjustedToUTC=false)`, see
  [Time period columns](#time-period-columns).

### Non-coded representations

| SDMX representation | Example | Parquet logical type | Parquet physical type |
| --- | --- | --- | --- |
| Alpha | `ABC` | `STRING` | `BYTE_ARRAY` |
| AlphaNumeric | `A1` | `STRING` | `BYTE_ARRAY` |
| BasicTimePeriod | `2020-01` | **multi column** | |
| BigInteger | `123456789` | `INT(64, signed)` | `INT64` |
| Boolean | `true` | — | `BOOLEAN` |
| Count | `42` | `INT(64, signed)` | `INT64` |
| DateTime | `2020-01-15T12:00:00Z` | `TIMESTAMP(MILLIS, isAdjustedToUTC=true)` | `INT64` |
| Day | `---15` | `INT(32, signed)` | `INT32` |
| Decimal | `3.14159` | — | `DOUBLE` |
| Double | `3.141592653589793` | — | `DOUBLE` |
| Duration | `P1Y2M3D` | `STRING` | `BYTE_ARRAY` |
| ExclusiveValueRange | `5.0` | — | `DOUBLE` |
| Float | `3.14` | — | `FLOAT` |
| GeospatialInformation | `{"type":"Point","coordinates":[2.3522,48.8566]}` | `STRING` (value as reported), plus [companion columns](#geospatial-columns) | `BYTE_ARRAY` |
| GregorianDay | `2020-01-15` | **multi column** | |
| GregorianTimePeriod | `2020-01-15` | **multi column** | |
| GregorianYear | `2020` | **multi column** | |
| GregorianYearMonth | `2020-01` | **multi column** | |
| InclusiveValueRange | `5.0` | — | `DOUBLE` |
| Incremental | `0.5` | — | `DOUBLE` |
| Integer | `42` | `INT(32, signed)` | `INT32` |
| Long | `9876543210` | `INT(64, signed)` | `INT64` |
| Month | `--11` | `INT(32, signed)` | `INT32` |
| MonthDay | `--01-15` | `STRING` | `BYTE_ARRAY` |
| Numeric | `00123` | `STRING` | `BYTE_ARRAY` |
| ObservationalTimePeriod | `2020-Q1` | **multi column** | |
| ReportingDay | `2020-D001` | **multi column** | |
| ReportingMonth | `2020-M01` | **multi column** | |
| ReportingQuarter | `2020-Q1` | **multi column** | |
| ReportingSemester | `2020-S1` | **multi column** | |
| ReportingTimePeriod | `2020-Q1` | **multi column** | |
| ReportingTrimester | `2020-T1` | **multi column** | |
| ReportingWeek | `2020-W01` | **multi column** | |
| ReportingYear | `2020-A1` | **multi column** | |
| Short | `32767` | `INT(16, signed)` | `INT32` |
| StandardTimePeriod | `2020-Q1` | **multi column** | |
| String | `Hello` | `STRING` | `BYTE_ARRAY` |
| Time | `12:30:00` | `TIME(MILLIS, isAdjustedToUTC=false)` | `INT32` |
| TimeRange | `2020-01-01/P3M` | **multi column** | |
| URI | `https://example.org` | `STRING` | `BYTE_ARRAY` |
| XHTML | `<p>Hello</p>` | `STRING` | `BYTE_ARRAY` |

- **Numeric** is a string of digits that can have leading zeros (e.g. `00123`),
  so it maps to `STRING`.
- **Day** and **Month** hold the day of the month (1–31) and the month (1–12)
  as integers. **Day**, **Month** and **MonthDay** are recurring calendar values without a
  year, not time periods, so they are single-column.
- **Time** values are local times: no time zone conversion applies.
- **XHTML** values are stored as-is: unlike in SDMX-CSV, no quoting or escaping
  applies.

## Time period columns

All time period types, i.e. the SDMX time data types except DateTime
(BasicTimePeriod, GregorianTimePeriod and its subtypes GregorianYear,
GregorianYearMonth and GregorianDay, ObservationalTimePeriod,
StandardTimePeriod, ReportingTimePeriod and its subtypes, TimeRange), are
represented by three columns:

| Column | Parquet logical type | Parquet physical type | Content |
| --- | --- | --- | --- |
| `{COMPONENT_ID}` | `STRING` | `BYTE_ARRAY` | The value as reported, e.g. `2020-Q1` |
| `_{COMPONENT_ID}_begin` | `TIMESTAMP(MILLIS, isAdjustedToUTC=false)` | `INT64` | Start of the period (inclusive) |
| `_{COMPONENT_ID}_end` | `TIMESTAMP(MILLIS, isAdjustedToUTC=false)` | `INT64` | End of the period (inclusive) |

- The type of period (year, quarter, range, etc.) is derived from the syntax
  of the reported value.
- `_begin` is the first instant of the period.
- `_end` is the last second of the period, i.e. the start of the following
  period minus one second. The period covers all instants `t` with
  `_begin <= t <= _end`.
- `_begin` and `_end` hold the local date and time of the period boundaries
  (`isAdjustedToUTC=false`): no time zone conversion applies.
- For reporting periods (e.g. `2020-Q1`, `2020-W01`), the start and end
  depend on the reporting year start day, which defaults to 1 January. The
  information needed to resolve them is provided with the data structure.
- For time ranges (e.g. `2020-01-01/P3M`), `_begin` is the reported start and
  `_end` is the start plus the duration, minus one second.
- For a date-time value (allowed in BasicTimePeriod, StandardTimePeriod and
  ObservationalTimePeriod), which is an instant rather than a period,
  `_begin` and `_end` both hold that date and time.

Examples:

| Reported value | Period | `_begin` | `_end` |
| --- | --- | --- | --- |
| `2020` | Year | `2020-01-01T00:00:00` | `2020-12-31T23:59:59` |
| `2020-Q1` | Quarter | `2020-01-01T00:00:00` | `2020-03-31T23:59:59` |
| `2020-01` | Month | `2020-01-01T00:00:00` | `2020-01-31T23:59:59` |
| `2020-01-15` | Day | `2020-01-15T00:00:00` | `2020-01-15T23:59:59` |
| `2020-01-01/P3M` | Time range | `2020-01-01T00:00:00` | `2020-03-31T23:59:59` |
| `2020-01-15T12:30:00` | Date-time | `2020-01-15T12:30:00` | `2020-01-15T12:30:00` |

## Geospatial columns

GeospatialInformation values, reported as GeoJSON, are stored as the original
string for round-tripping, as WKB for GeoParquet-compatible spatial queries,
and as convenience longitude/latitude columns for Point geometries and longitude/latitude/altitude for PointZ geometries:

| Column | Parquet logical type | Parquet physical type | Content |
| --- | --- | --- | --- |
| `{COMPONENT_ID}` | `STRING` | `BYTE_ARRAY` | The GeoJSON value as reported, e.g. `{"type":"Point","coordinates":[2.3522,48.8566]}` |
| `_{COMPONENT_ID}_geo` | — | `BYTE_ARRAY` | The geometry as WKB (Well-Known Binary) |
| `_{COMPONENT_ID}_X` | — | `DOUBLE` | Longitude, for Point and PointZ geometries; null otherwise, e.g. `2.3522` |
| `_{COMPONENT_ID}_Y` | — | `DOUBLE` | Latitude, for Point and PointZ geometries; null otherwise, e.g. `48.8566` |
| `_{COMPONENT_ID}_Z` | — | `DOUBLE` | Altitude, for PointZ geometries; null otherwise, e.g. `1880.12` |

- The `_geo` column MUST follow
  [GeoParquet 1.1](https://geoparquet.org/releases/v1.1.0): its description
  (encoding `WKB`, geometry types) is stored in the file-level key-value
  metadata under the key `geo`.
- GeoJSON coordinates are longitude and latitude in WGS 84
  ([RFC 7946](https://www.rfc-editor.org/rfc/rfc7946)), which corresponds to
  the GeoParquet default coordinate reference system (`OGC:CRS84`). The `crs`
  field MAY therefore be omitted.
- Note that for compatibility reasons with WKB, SDMX POINT type MUST be represented as POINTZ when it includes Altitude data.

## Values

### Nullability

As in SDMX 2.1 and SDMX 3.x data messages, every observation has a value for
each dimension, while measure and attribute values are optional:

| Columns | Nulls | Parquet repetition |
| --- | --- | --- |
| Dimension columns, including the time dimension and its `_begin` and `_end` companion columns | MUST NOT contain nulls | SHOULD be `required` |
| Measure and attribute columns, and their companion columns | MAY contain nulls: null means no value | `optional` |
| `_X` `_Y` `_Z`| Null for non-Point geometries | `optional` |
| Elements of a `LIST`, values of a `MAP` | MUST NOT be null | SHOULD be `required` |

- Receivers MUST accept dimension columns declared `optional` that contain no
  nulls.
- A mandatory attribute or measure (`usage="mandatory"` in SDMX 3.x,
  `assignmentStatus="Mandatory"` for attributes in SDMX 2.1) is still an
  `optional` column: whether mandatory values are present is checked by
  validation against the data structure, not by the format.

### Missing values

- A null value means that the component has no value for that observation.
- **Intentionally missing** values (represented in SDMX-CSV by `NaN` and
  `#N/A`) are not supported in SDMX-Parquet. A convention for them is under
  discussion with the SDMX Statistical Working Group (SWG), ideally at the
  level of the SDMX information model.

### Multi-valued components

- Multi-valued components are `LIST` columns, e.g. `["F1", "F2"]`.
- The element type is the mapped type of a single value.
- A component without values is null. Senders MUST NOT write an empty `LIST`.

### Multi-lingual components

Non-coded multi-lingual components map to a Parquet `MAP`:

| Part | Parquet logical type | Parquet physical type | Content |
| --- | --- | --- | --- |
| Key | `STRING` | `BYTE_ARRAY` | The language code, as used in SDMX-CSV: the ISO 639-1 code, e.g. `en`, `fr` |
| Value | `STRING` | `BYTE_ARRAY` | The text in that language |

- Example: `{"en": "Provisional", "fr": "Provisoire"}`, equivalent to the
  SDMX-CSV value `en:Provisional;fr:Provisoire`.
- Each language MUST appear at most once per map.
- A component without values is null. Senders MUST NOT write an empty `MAP`.
- Non-coded multi-lingual multi-valued components map to a `LIST` of such
  maps, one per value.

## Relation to SDMX REST

SDMX REST web services can return SDMX-Parquet for **data queries**, with the
media type:

`application/vnd.sdmx.data+parquet; version=1.0.0`

The queries supported with SDMX-Parquet, and the related responses, are
defined in the [SDMX REST API](https://github.com/sdmx-twg/sdmx-rest/blob/develop/docs/data.md#sdmx-parquet).

### Data actions and incremental updates

- Data actions (Merge, Replace, Delete) are not supported: a file contains
  data, not changes to data.
- Incremental updates (e.g. responses to `updatedAfter` queries) are not
  defined: without data actions, a file cannot represent deleted data.

## Implementation considerations

This section is not part of the SDMX-Parquet definition. It gives guidance
that senders can consider when producing files. Receivers MUST NOT rely on
it: a file that does not follow it is still a valid SDMX-Parquet file.

### Compression

- **ZSTD** is the recommended default codec: it gives small files with fast
  decompression, and is supported by current Parquet readers.
- **Snappy** is acceptable for compatibility with older readers.
- Compression is set per column chunk; using the same codec for all columns
  keeps files simple to produce and read.
- Most writers apply dictionary encoding automatically, which compresses
  repeated code IDs well without any configuration.

### Sorting

Sorting rows before writing improves both compression and query performance,
because repeated values end up next to each other and row group statistics
(min/max) become selective. A common choice is:

1. the dimensions other than the time dimension, in data structure order
   (i.e. by series key);
2. then the time dimension, by `_TIME_PERIOD_begin`.

This keeps all observations of a series together and in chronological
order, so readers can skip row groups when filtering on leading dimensions
or on time.

### Partitioning

The data can be delivered as several files (see
[Message overview](#message-overview)). Data warehouses and object stores
often keep large datasets this way, e.g. partitioned by a dimension in Hive
style:

```text
s3://bucket/ESTAT_NA_MAIN_1.6.0/REF_AREA=DE/data_0.parquet
s3://bucket/ESTAT_NA_MAIN_1.6.0/REF_AREA=FR/data_0.parquet
```

When SDMX-Parquet data is split this way:

- keep the partition columns inside each file, with their mapped types, so
  that every file has all component columns; Hive-style writers usually drop
  them and leave the values only in the directory names (in DuckDB, use the
  `WRITE_PARTITION_COLUMNS` option);
- write the same `sdmx.` keys in the metadata of every file;
- partition by a dimension with few distinct values, or by time, to avoid
  many small files;
- to deliver the data as a single file, combine the files into one, e.g. with
  DuckDB:

```sql
COPY (
  SELECT * FROM read_parquet('s3://bucket/ESTAT_NA_MAIN_1.6.0/**/*.parquet')
  ORDER BY FREQ, REF_AREA, _TIME_PERIOD_begin
) TO 'ESTAT_NA_MAIN_1.6.0.parquet' (
  FORMAT parquet, COMPRESSION zstd,
  KV_METADATA {'sdmx.structure': 'dataflow',
               'sdmx.structure_id': 'ESTAT:NA_MAIN(1.6.0)',
               'sdmx.format_version': '1.0.0'}
);
```

Within a single file, [sorting](#sorting) and [row groups](#row-groups)
already let readers skip the data they do not need, which covers many cases
where partitioning would otherwise be used.

### Row groups

- Row groups that are too small reduce compression and increase footer size;
  row groups that are too large reduce the benefit of statistics and
  increase memory use when writing and reading.
- Sizes in the order of 100 MB of uncompressed data, or about one million
  rows, are a common starting point.
- Keeping column statistics (min/max) enabled, the default in most writers,
  lets readers skip row groups.

## Example

Dataflow `ESTAT:NA_MAIN(1.6.0)` with:

- dimensions `FREQ` and `REF_AREA` (coded), and `TIME_PERIOD`
  (ObservationalTimePeriod);
- measure `OBS_VALUE` (Double);
- attributes `OBS_STATUS` (coded), `UNIT_MULT` (Integer, attached at series
  level), `FOOTNOTES` (coded, multi-valued) and `COMMENT` (String,
  multi-lingual).

**Parquet schema** of the example file (timestamp flags abridged):

```text
required group field_id=-1 schema {
  required binary field_id=-1 FREQ (String);
  required binary field_id=-1 REF_AREA (String);
  required binary field_id=-1 TIME_PERIOD (String);
  required int64 field_id=-1 _TIME_PERIOD_begin (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds));
  required int64 field_id=-1 _TIME_PERIOD_end (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds));
  optional double field_id=-1 OBS_VALUE;
  optional binary field_id=-1 OBS_STATUS (String);
  optional int32 field_id=-1 UNIT_MULT;
  optional group field_id=-1 FOOTNOTES (List) {
    repeated group field_id=-1 list {
      required binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 COMMENT (Map) {
    repeated group field_id=-1 key_value {
      required binary field_id=-1 key (String);
      required binary field_id=-1 value (String);
    }
  }
}
```

**File key-value metadata:**

| Key | Value |
| --- | --- |
| `sdmx.structure` | `dataflow` |
| `sdmx.structure_id` | `ESTAT:NA_MAIN(1.6.0)` |
| `sdmx.format_version` | `1.0.0` |
| `sdmx.as_of` | `2026-10-20T08:00:00Z` |
| `sdmx.sender` | `ESTAT` |

**Content:**

| FREQ | REF_AREA | TIME_PERIOD | _TIME_PERIOD_begin | _TIME_PERIOD_end | OBS_VALUE | OBS_STATUS | UNIT_MULT | FOOTNOTES | COMMENT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | DE | 2020 | 2020-01-01T00:00:00 | 2020-12-31T23:59:59 | 3.2 | A | 6 | null | {"en": "Revised"} |
| D | FR | 2020-01-15 | 2020-01-15T00:00:00 | 2020-01-15T23:59:59 | 0.4 | A | 6 | null | null |
| Q | FR | 2020-Q1 | 2020-01-01T00:00:00 | 2020-03-31T23:59:59 | 12.4 | A | 6 | ["F1", "F2"] | {"en": "Provisional", "fr": "Provisoire"} |
| Q | FR | 2020-Q2 | 2020-04-01T00:00:00 | 2020-06-30T23:59:59 | 10.8 | P | 6 | null | null |

`UNIT_MULT`, attached at series level, is repeated on each observation row.
Rows are sorted by series key and then by time, as described in
[Sorting](#sorting).

### Reading examples

The file can be read with common data analytics tools, without any
SDMX-specific library.

**Python (pandas):**

```python
import pandas as pd
import pyarrow.parquet as pq

df = pd.read_parquet("example.parquet")
quarterly = df[df["FREQ"] == "Q"][["REF_AREA", "TIME_PERIOD", "OBS_VALUE"]]

# SDMX file metadata
meta = pq.read_metadata("example.parquet").metadata
print(meta[b"sdmx.structure_id"])  # b'ESTAT:NA_MAIN(1.6.0)'
```

**Python (Polars):**

```python
import polars as pl

df = pl.read_parquet("example.parquet")
quarterly = df.filter(pl.col("FREQ") == "Q").select("REF_AREA", "TIME_PERIOD", "OBS_VALUE")
```

**R:**

```r
library(arrow)

df <- read_parquet("example.parquet")
quarterly <- subset(df, FREQ == "Q", select = c(REF_AREA, TIME_PERIOD, OBS_VALUE))
```

**SQL (DuckDB):**

```sql
SELECT REF_AREA, TIME_PERIOD, OBS_VALUE
FROM 'example.parquet'
WHERE FREQ = 'Q' AND _TIME_PERIOD_begin >= TIMESTAMP '2020-01-01';

-- SDMX file metadata
SELECT key, value FROM parquet_kv_metadata('example.parquet');
```

**Apache Spark (PySpark):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("example.parquet")
df.filter(df.FREQ == "Q").select("REF_AREA", "TIME_PERIOD", "OBS_VALUE").show()
```
