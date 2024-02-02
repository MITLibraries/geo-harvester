# OpenGeoMetadata (OGM) Harvests

The following are sequence diagrams related to harvesting OpenGeoMetadata (OGM) data.

## Full Harvest

A full harvest from OGM would include retrieving metadata from ALL repositories, and ALL metadata file formats, we are configured to harvest from.

```mermaid
sequenceDiagram
    autonumber
    participant ogm_config_yaml as OGM Config YAML
    participant geo_harv as GeoHarvester
    participant ogm_repo as OGM Institution Repository
    participant s3_cdn_pub as S3:CDN:Public
    participant s3_timdex as S3:TIMDEX
    
    geo_harv->>ogm_config_yaml: Load OGM harvest config YAML
    ogm_config_yaml-->>geo_harv: Detailed list of repositories, paths, <br> and formats
    
    loop For each repository
        geo_harv->>ogm_repo: Clone repo
        ogm_repo-->>geo_harv: Repo files
        geo_harv->>geo_harv: Filter to list of files based on <br> supported metadata formats from config
        geo_harv->>geo_harv: Normalize source metadata to Aardvark
        geo_harv->>s3_cdn_pub: Write source and MIT aardvark metadata
        geo_harv->>s3_timdex: Write MIT aardvark
    end
```

## Daily (Incremental) Harvest

A daily (incremental) will rely on git commits to pickup changes to repositories after the run date specified.

```mermaid
sequenceDiagram
    autonumber
    participant ogm_config_yaml as OGM Config YAML
    participant geo_harv as GeoHarvester
    participant ogm_repo as OGM Institution Repository
    participant s3_cdn_pub as S3: CDN:Public/OGM
    participant s3_timdex as S3: TIMDEX
    
    geo_harv->>ogm_config_yaml: Load OGM harvest config YAML
    ogm_config_yaml-->>geo_harv: Detailed list of repositories, paths, <br> and formats
    
    loop GitHub Repository
        geo_harv->>ogm_repo: Clone repo
        geo_harv->>ogm_repo: Get list of commits after X date
        ogm_repo-->>geo_harv: Git commits
        geo_harv->>geo_harv: Parse commits and determine metadata <br> files that were modified or deleted
        geo_harv->>geo_harv: Filter to list of files based on <br> supported metadata formats from config
        geo_harv->>s3_timdex: Write MIT aardvark
    end
```