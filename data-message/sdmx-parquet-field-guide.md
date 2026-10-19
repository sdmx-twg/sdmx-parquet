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

Because the schema and the row group index are in the footer, a sender can
write row groups incrementally, but a receiver needs access to the end of the
file (the complete file, or random access such as HTTP range requests) before
reading data.

## Message overview

An SDMX-Parquet data message is a single Parquet file containing the complete
data of **one dataflow**. Each row contains the information related to **one
observation**: its dimension values, measure values and attribute values.
Values of attributes attached to a partial key (series, group or dataset
level) are repeated on each related observation row.

### Column overview

| Column name | Description | Required |
| --- | --- | --- |
| **Component columns** | | |
| [`COMPONENT_ID`](#component-columns), e.g. `REF_AREA`, `OBS_VALUE` | One column per dimension, measure and attribute of the data structure definition, named by its component ID. For multi-column types, holds the value as reported. | Yes, for all components |
| **Companion columns** (multi-column types) | | |
| [`_COMPONENT_ID_begin`](#time-period-columns) | Start of the reported time period. | Yes, for time period types |
| [`_COMPONENT_ID_end`](#time-period-columns) | End of the reported time period (exclusive). | Yes, for time period types |
| [`_COMPONENT_ID_geo`](#geospatial-columns) | Geometry. | Under discussion |
| [`_COMPONENT_ID_longitude`](#geospatial-columns) | Longitude of point geometries. | Under discussion |
| [`_COMPONENT_ID_latitude`](#geospatial-columns) | Latitude of point geometries. | Under discussion |
| **Other columns** | | |
| [Custom column](#custom-columns) | Any implementer-defined column. | No |

There are no columns for the structure identification or the action: the
dataflow is identified in the [file metadata](#file-metadata), and
[data actions](#data-actions-and-incremental-updates) are not supported.
Labels are not included (IDs only).

## Design principles

- **One dataflow per file:** the file contains the data of a single dataflow.
- **Complete content:** the file contains the entire existing data of the
  dataflow. Filtering is not supported (see
  [Relation to SDMX REST](#relation-to-sdmx-rest)).
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

This location is used because:

- every Parquet reader exposes it, and it can be read without reading any
  data;
- it describes the file as a whole, unlike column-level key-value metadata,
  which is stored per column chunk and is rarely exposed by tools;
- it travels with the file, unlike a separate sidecar file.

Senders MUST write the following keys:

| Key | Content | Example |
| --- | --- | --- |
| `sdmx.structure` | Type of structure. MUST be `dataflow`. | `dataflow` |
| `sdmx.structure_id` | Dataflow identification `AGENCY:ID(VERSION)`, as in SDMX-CSV `STRUCTURE_ID`. For non-versioned artefacts the version is omitted, e.g. `AGENCY:DF_ID`. | `ESTAT:NA_MAIN(1.6.0)` |
| `sdmx.format_version` | Version of the SDMX-Parquet specification. | `1.0.0` |

- Keys starting with `sdmx.` are reserved for this specification.
- Senders MAY add other keys. Keys defined by other conventions are
  unaffected, e.g. `geo` for [GeoParquet](#geospatial-columns).
- Receivers MUST reject a file whose `sdmx.structure` value is not
  `dataflow`.

## Columns

### Component columns

- Each column is named by the ID of its SDMX component.
- The file MUST include a column for every component defined by the data
  structure definition: dimensions, measures and attributes. A column with no
  values for any row contains only nulls.
- Metadata attributes are not supported: they are defined in a metadata
  structure definition, not in the data structure definition of the
  dataflow. The file MUST NOT include columns for them.
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
- All columns SHOULD be declared `optional` (nullable), see
  [Missing values](#missing-values).
- `TIMESTAMP` values MUST use the unit `MILLIS` and MUST be adjusted to UTC
  (`isAdjustedToUTC = true`).

### Non-coded representations

| SDMX representation | Example | Parquet logical type | Parquet physical type |
| --- | --- | --- | --- |
| Alpha | `ABC` | `STRING` | `BYTE_ARRAY` |
| AlphaNumeric | `A1` | `STRING` | `BYTE_ARRAY` |
| BasicTimePeriod | `2020-01` | **multi column** | |
| BigInteger | `123456789` | `INT(64, signed)` | `INT64` |
| Boolean | `true` | — | `BOOLEAN` |
| Count | `42` | `INT(32, signed)` | `INT32` |
| DateTime | `2020-01-15T12:00:00Z` | `TIMESTAMP(MILLIS, UTC)` | `INT64` |
| Day | `---15` | `INT(32, signed)` | `INT32` |
| Decimal | `3.14159` | `DECIMAL` | `BYTE_ARRAY` |
| Double | `3.141592653589793` | — | `DOUBLE` |
| Duration | `P1Y2M3D` | `STRING` | `BYTE_ARRAY` |
| ExclusiveValueRange | `5.0` | `DECIMAL` | `BYTE_ARRAY` |
| Float | `3.14` | — | `FLOAT` |
| GeospatialInformation | see [Geospatial columns](#geospatial-columns) | **multi column** | |
| GregorianDay | `15` | `INT(32, signed)` | `INT32` |
| GregorianTimePeriod | `2020-01-15` | **multi column** | |
| GregorianYear | `2020` | `INT(32, signed)` | `INT32` |
| GregorianYearMonth | `2020-01` | **multi column** | |
| InclusiveValueRange | `5.0` | `DECIMAL` | `BYTE_ARRAY` |
| Incremental | `0.5` | `DECIMAL` | `BYTE_ARRAY` |
| Integer | `42` | `INT(32, signed)` | `INT32` |
| Long | `9876543210` | `INT(64, signed)` | `INT64` |
| Month | `--11` | `INT(32, signed)` | `INT32` |
| MonthDay | `--01-15` | `STRING` | `BYTE_ARRAY` |
| Numeric | `123.45` | — | `DOUBLE` |
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
| Time | `12:30:00` | `TIME(MILLIS)` | `INT32` |
| TimeRange | `2020-01-01/P3M` | **multi column** | |
| URI | `https://example.org` | `STRING` | `BYTE_ARRAY` |
| XHTML | `<p>Hello</p>` | `STRING` | `BYTE_ARRAY` |

Day and Month hold the day of the month (1–31) and the month (1–12) as
integers. XHTML values are stored as-is: unlike in SDMX-CSV, no quoting or
escaping applies.

!!! warning "Open point: BigInteger"

    SDMX BigInteger is arbitrary-precision; `INT64` is limited to
    ±9.2 × 10¹⁸. Options:

    1. `INT64` (as above): native integer arithmetic in all tools; values
       outside the range cannot be represented.
    2. `DECIMAL(38, 0)`: up to 38 digits, exact; slower arithmetic, and not
       all tools support it equally.
    3. `STRING`: no limit; loses numeric typing.

!!! warning "Open point: Decimal precision and scale"

    The Parquet `DECIMAL` type requires a precision and a scale, which the
    data structure definition does not always provide. Options:

    1. A fixed default for all decimal components, e.g. `DECIMAL(38, 10)`:
       simple and predictable; may round values with more than 10 decimal
       places or waste space.
    2. Derived from the representation facets of the data structure
       definition (e.g. maximum length and number of decimals) when present,
       with a fixed default otherwise: exact where facets exist; requires a
       defined derivation rule.
    3. `DOUBLE`: universally supported; not exact for decimal values.

## Time period columns

All time period types (BasicTimePeriod, GregorianTimePeriod and its subtypes,
ObservationalTimePeriod, StandardTimePeriod, ReportingTimePeriod and its
subtypes, TimeRange) are represented by three columns:

| Column | Parquet logical type | Parquet physical type | Content |
| --- | --- | --- | --- |
| `{COMPONENT_ID}` | `STRING` | `BYTE_ARRAY` | The value as reported, e.g. `2020-Q1` |
| `_{COMPONENT_ID}_begin` | `TIMESTAMP(MILLIS, UTC)` | `INT64` | Start of the period (inclusive) |
| `_{COMPONENT_ID}_end` | `TIMESTAMP(MILLIS, UTC)` | `INT64` | End of the period (exclusive) |

- The type of period (year, quarter, range, etc.) is derived from the syntax
  of the reported value.
- The period covers all instants `t` with `_begin <= t < _end`. `_end` is
  therefore the instant at which the period ends, which is also the start of
  the following period, e.g. for `2020-Q1`: `_begin` = `2020-01-01T00:00:00Z`
  and `_end` = `2020-04-01T00:00:00Z`.
- For reporting periods (e.g. `2020-Q1`, `2020-W01`), the start and end
  depend on the reporting year start day, which defaults to 1 January. The
  information needed to resolve them is provided with the data structure.
- For time ranges (e.g. `2020-01-01/P3M`), `_begin` is the reported start and
  `_end` is the start plus the duration.

## Geospatial columns

!!! question "Under discussion"

    The representation of GeospatialInformation values is being defined with
    geospatial specialists. The current draft uses four columns:

    | Column | Content |
    | --- | --- |
    | `{COMPONENT_ID}` | The value as reported, for round-tripping |
    | `_{COMPONENT_ID}_geo` | Geometry as WKB (Well-Known Binary), for spatial queries |
    | `_{COMPONENT_ID}_longitude` | Longitude, for point geometries only |
    | `_{COMPONENT_ID}_latitude` | Latitude, for point geometries only |

!!! warning "Open points"

    - **GeoParquet version and logical type:** GeoParquet metadata is stored
      in the file-level key-value metadata under the key `geo`. Recent
      versions of the Parquet format define native `GEOMETRY` and `GEOGRAPHY`
      logical types, which GeoParquet 2.0 requires. Which GeoParquet version,
      and which logical type for `_geo`, should SDMX-Parquet target?
    - **Encoding of the reported value:** the encoding of SDMX
      GeospatialInformation values as reported must be confirmed, and one
      example used throughout. Earlier drafts used WKT (`POINT(0.5 51.5)`) in
      one place and GeoJSON (`{"type":"Point",...}`) in another.
    - **Non-point geometries:** are `_longitude` and `_latitude` null for
      non-point geometries, or should a representative point (e.g. the
      centroid) be provided?
    - **Coordinate reference system:** which CRS is assumed or declared?

## Values

### Missing values

- A null value means that the component has no value for that observation.
- **Intentionally missing** values (represented in SDMX-CSV by `NaN` and
  `#N/A`) are not supported in SDMX-Parquet. A convention for them is under
  discussion with the SDMX Statistical Working Group (SWG), ideally at the
  level of the SDMX information model.

!!! warning "Open point"

    The meaning of a null `LIST` versus an empty `LIST` must be defined
    (e.g. both meaning "no value", or an empty `LIST` being disallowed).

### Multi-valued components

- Multi-valued components are `LIST` columns, e.g. `["F1", "F2"]`.
- The element type is the mapped type of a single value.

### Multi-lingual components

- Non-coded multi-lingual components map to a Parquet `MAP` with `STRING`
  keys and `STRING` values. The key is the ISO 639-1 language code, the value
  is the text in that language, e.g. `{"en": "Provisional", "fr":
  "Provisoire"}`.
- Each language MUST appear at most once per map.
- Non-coded multi-lingual multi-valued components map to a `LIST` of such
  maps, one per value.

## Relation to SDMX REST

### Media type

SDMX REST data queries can return SDMX-Parquet with the media type:

`application/vnd.sdmx.data+parquet; version=1.0.0`

This media type is being added to the
[SDMX REST API](https://github.com/sdmx-twg/sdmx-rest/blob/master/doc/data.md).
It has no parameters other than `version`.

### Supported queries

Because a file always contains the complete data of one dataflow, an
SDMX-Parquet response can only be returned for a data query that identifies
exactly one dataflow and applies no filter.

When a request asks for `application/vnd.sdmx.data+parquet` together with an
unsupported path value, query parameter or media type parameter, the web
service SHOULD return **`406 Not Acceptable`**, as defined in
[SDMX REST status codes](https://github.com/sdmx-twg/sdmx-rest/blob/master/doc/status.md)
for a resource representation that the web service does not offer. The error
message SHOULD name the unsupported parameter(s). The query itself is valid,
so `400 Bad Request` does not apply: the same query can be answered with
another format, e.g. SDMX-CSV.

| Request element | Supported with SDMX-Parquet | Response when not supported |
| --- | --- | --- |
| `context` | `dataflow` only | `406` |
| `agencyID`, `resourceID`, `version` | Must identify exactly one dataflow | `406` |
| `key` | All values only (`*` or omitted) | `406` |
| `c` | No | `406` |
| `updatedAfter` | No | `406` |
| `firstNObservations`, `lastNObservations` | No | `406` |
| `attributes` | Default value only (`dsd`); `msd` and `all` are not supported, as metadata attributes are not included | `406` |
| `measures` | Default value only (`all`) | `406` |
| `includeHistory` | Default value only (`false`) | `406` |
| `asOf` | No | `406` |
| `offset`, `limit` | No | `406` |
| `sort` | No | `406` |
| `reportingYearStartDay` | No | `406` |
| `dimensionAtObservation` | Not applicable: the file is always flat | Ignored |
| `Accept` media type parameters other than `version` (e.g. `labels`, `timeFormat`, `keys`) | No | `406` |
| `Accept-Language` header | Not applicable: IDs only | Ignored |

### Data actions and incremental updates

- Data actions (Merge, Replace, Delete) are not supported: a file contains
  the complete current data of the dataflow.
- Incremental updates (e.g. responses to `updatedAfter` queries) are not
  supported.

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
  optional binary field_id=-1 FREQ (String);
  optional binary field_id=-1 REF_AREA (String);
  optional binary field_id=-1 TIME_PERIOD (String);
  optional int64 field_id=-1 _TIME_PERIOD_begin (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds));
  optional int64 field_id=-1 _TIME_PERIOD_end (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds));
  optional double field_id=-1 OBS_VALUE;
  optional binary field_id=-1 OBS_STATUS (String);
  optional int32 field_id=-1 UNIT_MULT;
  optional group field_id=-1 FOOTNOTES (List) {
    repeated group field_id=-1 list {
      optional binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 COMMENT (Map) {
    repeated group field_id=-1 key_value {
      required binary field_id=-1 key (String);
      optional binary field_id=-1 value (String);
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

**Content:**

| FREQ | REF_AREA | TIME_PERIOD | _TIME_PERIOD_begin | _TIME_PERIOD_end | OBS_VALUE | OBS_STATUS | UNIT_MULT | FOOTNOTES | COMMENT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q | FR | 2020-Q1 | 2020-01-01T00:00:00Z | 2020-04-01T00:00:00Z | 12.4 | A | 6 | ["F1", "F2"] | {"en": "Provisional", "fr": "Provisoire"} |
| Q | FR | 2020-Q2 | 2020-04-01T00:00:00Z | 2020-07-01T00:00:00Z | 10.8 | P | 6 | [] | null |
| A | DE | 2020 | 2020-01-01T00:00:00Z | 2021-01-01T00:00:00Z | 3.2 | A | 6 | null | {"en": "Revised"} |

`UNIT_MULT`, attached at series level, is repeated on each observation row.

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
