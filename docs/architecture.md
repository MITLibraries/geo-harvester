# Geo Harvester Architecture

## Classes

### Harvesters

```mermaid
classDiagram
    class CLI{
        harvest.mit()
        harvest.ogm()
    }
    class Harvester{
        harvest()*
    }
    class MITHarvester{
    }
    class OGMHarvester{
    }
    class SQSClient {
        get_valid_messages_iter()
    }
    class EventBridgeClient {
        send_event()
    }
    class S3Client {
        list_objects()
        read_file_from_zip()
        write_file()
    }
    class GithubClient {
        list_repositories() -> list
        get_commits() -> list
        read_file_from_commit(filename, commit) -> bytes
        clone_repository() -> files
    }
    
    CLI <-- Harvester
    Harvester <|-- MITHarvester
    Harvester <|-- OGMHarvester
    Harvester <-- EventBridgeClient
    MITHarvester <-- SQSClient
    MITHarvester <-- S3Client
    OGMHarvester <-- GithubClient
    Harvester <-- S3Client
```

### Metadata Normalization

```mermaid
classDiagram
    class Record{
        source: SourceRecord
        normalized: MITAardvark
    }
    class SourceRecord{
        transform(self)* -> MITAardvark
    }
    class MITAardvark{
    }
    class FGDC{
        xml_tree: lxml.ElementTree
        transform(self) -> MITAardvark
    }
    class ISO19139{
        xml_tree: lxml.ElementTree
        transform(self) -> MITAardvark
    }
    class GBL{
        data: JSON
        transform(self) -> MITAardvark
    }
    class Aardvark{
        data: JSON
        transform(self) -> MITAardvark
    }
    
    Record <-- SourceRecord
    Record <-- MITAardvark
    SourceRecord <|-- FGDC
    SourceRecord <|-- ISO19139
    SourceRecord <|-- GBL
    SourceRecord <|-- Aardvark
```

- `Record`
  - class that represents a single geo resource
  - has attributes `source` and `normalized` to represent its original (source) and normalized form
- `SourceRecord`
  - class that provides data and functionality for the source record
  - extended by other classes that define how to normalize to MITAardvark
- `MITAardvark`
  - a normalized record can be serialized as an MIT Aardvark record

## Entrypoints and Flow

The primary entrypoint for CLI commands will be a `Harvester` instance, which depending on the CLI command `harvester harvest mit` or `harvester harvest ogm`, will either be a `MITHarvester` or `OGMHarvester` respectively.

The `Harvester` class has a primary entrypoint `harvest` method that kicks off a series of steps:

```mermaid
flowchart TD
    get[Get source records]
    normalize[Normalize source records]
    write_public[Write ALL metadata to S3:CDN:Public]
    write_timdex[Write SOME metadata to S3:TIMDEX]
    harv_spec[MIT or OGM Harvester specific work]
    report[Report]
    
    get --> normalize
    normalize --> write_public
    write_public --> write_timdex
    write_timdex --> harv_spec
    harv_spec --> report    
```

The data passed between each step is an `Iterator` of `Record` instances, where each `Record` instance contains both a `SourceRecord` and `MITAardvark` instance attached, thereby connecting the two throughout the work.

These flows are detailed more in [MIT Harvests](mit_harvests.md) and [OpenGeoMetadata (OGM) Harvests](ogm_harvests.md).

## Separation of Concerns

Similar to other TIMDEX harvesters like [oai-pmh-harvester](https://github.com/MITLibraries/oai-pmh-harvester) and [browsertrix-harvester](https://github.com/MITLibraries/browsertrix-harvester), this harvester's responsibility is preparing metadata records, in a single format, based on data extracted from a source, and writing to a known location.

In the case of the OAI harvester, the data _is_ metadata and it _is_ already normalized.  Browsertrix performs a web crawl which provides _raw_ data about websites, which is then normalized to a custom XML metadata format.  This harvester will work from metadata records either in S3 (from MIT) or in Github repositories (OGM), but these "source" metadata records will be in a variety of formats.  Therefore, an important duty this harvester performs is normalization to a single format (Aardvark).

All that said, this harvester is unique one important way: **for MIT records, there is connected work of managing actual GIS zip files in S3 based on the results of metadata normalization**.  It is only after this normalization process -- the first time we look closely at the original source metadata -- that we understand if the GIS zip files are public or restricted in nature.  It was decided that this harvester should NOT be responsible for performing this work, but WILL be responsible for sending an event that informs another process to do so.

A StepFunction -- name and code TBD -- will be created that is responsible for managing the actual GIS zip files.  This StepFunction will be providing SQS messages that this harvester READS, informing it what files have been modified and therefore should be harvested for, or removed from, TIMDEX.  Then, after normalization to Aardvark, this harvester will SEND an EventBridge event that informs that StepFunction what further file actions are needed.  In this way, this harvester is responds to and triggers file actions, but does not perform them.
