# Introduction

SDMX-Parquet data message is an SDMX data format based on the IANA media type [vnd.apache.parquet](https://www.iana.org/assignments/media-types/application/vnd.apache.parquet). The Parquet file format is an efficient, general-purpose, column-oriented data format used across a wide variety of platforms, technologies, and environments. 

See more on the [parquet documentation](https://parquet.apache.org/docs/), to find more about the general parquet format, its characteristics, and features.

SDMX-Parquet format is designed as a tabular representation (SDMX flat) for data analytics use cases. It is optimized for storage, query performance. 

## Design principles for SDMX-Parquet 1.0 data messages

### Format characteristics
- Structures - Information about the structure(s) can be provided in the metadata section.
- Labels - should be ids only
- Packaged in tabular format (SDMX flat representation)
- Streamable - A message is streamable
- Cube components -The combination of dimension values can be expressed as cube components.
- Strong typing & Native data types - The combination of dimension values can be expressed as cube components.
- Attributes with multiple values - The combination of dimension values can be expressed as cube components.
- Readable by non-SDMX sofware for analysis -The combination of dimension values can be expressed as cube components.
- The data contents are dataflow specific and contain the entire existing data (Filtering at the Rest API level is not supported).
  
### File structure
- Column names - Each column is named by its SDMX component id. The table includes all SDMX components defined by the DSD: dimensions, measures and attributes.
- Column types - Strong typing: every SDMX representation maps to a specific Parquet logical and physical type for consistency and
performance (see more in the section format mapping below).
- Mandatory columns - Multi-column representations (time periods, geospatial) expand into their mandatory companion columns, as detailed
in the multi-column mappings.
- Extra columns - Extra columns are allowed and do not invalidate it as a SDMX-Parquet file.

### Format mapping
The following matrix represents the SDMX component representation type in SDMX and its mapping with parquet. 

- Rows whose logical type reads 'multi-column' expand into several Parquet columns; those expansions are listed in the
multi-column mappings below.

|  SDMX                     | SDMX Primitive | SDMX Example            | Parquet Logical Type | Parquet Type |
|---------------------------|----------------|-------------------------|----------------------|--------------|
| Alpha                     | String         | ABC                     | String               | Byte Array   |
| Alpha Numeric             | String         | A1                      | String               | Byte Array   |
| Basic Time Period         | SdmxDate       | 2020-Q1                 | ***multi column***   |              |
| Big Integer               | Int            | 123456789               | Int64                | Int64        |
| Boolean                   | Boolean        | true                    | Boolean              | Boolean      |
| Count                     | Int            | 42                      | Int32                | Int32        |
| Date Time                 | DateTime       | 2020-01-15T12:00:00     | Timestamp            | Int64        |
| Day                       | Int            | 15                      | Int32                | Int32        |
| Decimal                   | Decimal        | 3.14159                 | Decimal              | Byte Array   |
| Double                    | Double         | 3.141592653589793       | Double               | Double       |
| Duration                  | String         | P1Y2M3D                 | String               | Byte Array   |
| Exclusive Value Range     | Decimal        | 5.0                     | Decimal              | Byte Array   |
| Float                     | Float          | 3.14                    | Float                | Float        |
| Geospatial Information    | GeoJSON        | POINT(0.5 51.5)         | ***multi column***   |              |
| Gregorian Day             | Int            | 15                      | Int32                | Int32        |
| Gregorian Time Period     | SdmxDate       | 2020-01-15              | ***multi column***   |              |
| Gregorian Year            | Int            | 2020                    | Int32                | Int32        |
| Gregorian Year Month      | SdmxDate       | 2020-01                 | ***multi column***   |              |
| Inclusive Value Range     | Decimal        | 5.0                     | Decimal              | Byte Array   |
| Incremental               | Decimal        | 0.5                     | Decimal              | Byte Array   |
| Integer                   | Int            | 42                      | Int32                | Int32        |
| Long                      | Int            | 9876543210              | Int64                | Int64        |
| Month                     | Int            | 11                      | Int32                | Int32        |
| Month Day                 | String         | --01-15                 | String               | Byte Array   |
| Numeric                   | Double         | 123.45                  | Double               | Double       |
| Observational Time Period | SdmxDate       | 2020-Q1                 | ***multi column***   |              |
| Reporting Day             | SdmxDate       | 2020-D001               | ***multi column***   |              |
| Reporting Month           | SdmxDate       | 2020-M01                | ***multi column***   |              |
| Reporting Quarter         | SdmxDate       | 2020-Q1                 | ***multi column***   |              |
| Reporting Semester        | SdmxDate       | 2020-S1                 | ***multi column***   |              |
| Reporting Time Period     | SdmxDate       | 2020-Q1                 | ***multi column***   |              |
| Reporting Trimester       | SdmxDate       | 2020-T1                 | ***multi column***   |              |
| Reporting Week            | SdmxDate       | 2020-W01                | ***multi column***   |              |
| Reporting Year            | SdmxDate       | 2020-A1                 | ***multi column***   |              |
| Short                     | Int            | 32767                   | Int16                | Int32        |
| Standard Time Period      | SdmxDate       | 2020-Q1                 | ***multi column***   |              |
| String                    | String         | Hello                   | String               | Byte Array   |
| Time                      | Time           | 12:30:00                | Time                 | Int32        |
| Time Range                | SdmxDate       | 2020-Q1/2020-Q4         | ***multi column***   |              |
| URI                       | String         | https://example.org     | String               | Byte Array   |
| XHTML                     | String         | \<p\>Hello\</p\>        | String               | Byte Array   |


**Multi Column Mapping**

Time values are resolved, the syntax of the reported date provides enough information to derive the type i.e. a Quarterly date, a 
Range, etc. The DSD contains the information used to resolve the reporting start and end periods for Reporting Periods


| SDMX Primitive | SDMX Example            | Parquet Logical Type | Parquet Type |  Description |
|----------------|-------------------------|----------------------|--------------|--------------|
| SdmxDate       | 2020-Q1                 | String               | Byte Array   | As reported  |
|                |                         | Timestamp            | Int64        | Start Period |
|                |                         | Timestamp            | Int64        | End Period   |

GeoJSON values are stored as the original string for round-tripping, as WKB for GeoParquet-compatible spatial queries, and as convenience lat/lon columns for Point geometries.

Q) Should the parquet logical type say GeoParquet?                                                                                                                                                                                                                                                                                                                         

A) No — GeoParquet is not a Parquet logical type. It's a metadata convention that sits above the Parquet type system. The WKB column has no formal Parquet logical type annotation (None / raw bytes); GeoParquet compliance is expressed through column-level metadata (a JSON object describing the CRS, geometry types, bounding box etc.) attached to the column, not
  through the logical type field.

  None is the correct entry for the Parquet Logical Type — the Description column already carries the GeoParquet reference, which is the right place for it.



| SDMX Primitive | SDMX Example            | Parquet Logical Type | Parquet Type |  Description                        |
|----------------|-------------------------|----------------------|--------------|-------------------------------------|
| GeoJSON        | {"type":"Point",...}    | String               | Byte Array   | As reported (GeoJSON)               |
|                |                         | None                 | Byte Array   | WKB geometry (GeoParquet)           |
|                |                         | Double               | Double       | Longitude (Point geometries only)   |
|                |                         | Double               | Double       | Latitude (Point geometries only)    |

