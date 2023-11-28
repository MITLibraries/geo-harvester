# OpenGeoMetadata (OGM) Harvests

The following are sequence diagrams related to harvesting OpenGeoMetadata (OGM) data.

## Full Harvest

A full harvest from OGM would include retrieving metadata from ALL repositories, and ALL metadata file formats, we are configured to harvest from.

```mermaid
sequenceDiagram
    autonumber
    participant ogm_config_yaml as OGM Config YAML
    participant gis_harv as GIS Harvester
    participant ogm_repo as OGM Institution Repository
    participant s3_cdn_pub as S3: CDN:Public/OGM
    participant s3_timdex as S3: TIMDEX
    
    gis_harv->>ogm_config_yaml: Load OGM harvest config YAML
    ogm_config_yaml-->>gis_harv: Detailed list of repositories, paths, <br> and formats
    
    gis_harv->>s3_cdn_pub: DELETE ALL source and aardvark metadata
    
    loop For each repository
        gis_harv->>ogm_repo: Clone repo
        ogm_repo-->>gis_harv: Repo files
        gis_harv->>gis_harv: Filter to list of files based on <br> supported metadata formats from config
        gis_harv->>gis_harv: Normalize source metadata to Aardvark
        gis_harv->>s3_cdn_pub: Write source and aardvark metadata
        gis_harv->>s3_timdex: Write "to-index" JSON records
    end
```

## Daily (Incremental) Harvest

A daily (incremental) will rely on git commits to pickup changes to repositories after the run date specified.

```mermaid
sequenceDiagram
    autonumber
    participant ogm_config_yaml as OGM Config YAML
    participant gis_harv as GIS Harvester
    participant ogm_repo as OGM Institution Repository
    participant s3_cdn_pub as S3: CDN:Public/OGM
    participant s3_timdex as S3: TIMDEX
    
    gis_harv->>ogm_config_yaml: Load OGM harvest config YAML
    ogm_config_yaml-->>gis_harv: Detailed list of repositories, paths, <br> and formats
    
    loop GitHub Repository
        gis_harv->>ogm_repo: Get list of commits after X date
        ogm_repo-->>gis_harv: Git commits
        gis_harv->>gis_harv: Parse commits and determine metadata <br> files that were modified or deleted
        gis_harv->>gis_harv: Filter to list of files based on <br> supported metadata formats from config
        
        loop Modified files
            gis_harv->>ogm_repo: Request metadata file
            ogm_repo-->>gis_harv: Metadata file
            Note right of gis_harv: Need to still normalize to aardvark <br> to get meaningful identifier
            gis_harv->>gis_harv: Normalize source metadata to Aardvark
            alt Action: Created or Modified
                gis_harv->>s3_cdn_pub: WRITE source and aardvark metadata (may overwrite)
                gis_harv->>gis_harv: Add to "to-index" list in memory
            else Action: Deleted
                gis_harv->>s3_cdn_pub: DELETE source and aardvark metadata
                gis_harv->>gis_harv: Add to "to-delete" list in memory
            end
        end
        
        gis_harv->>s3_timdex: Write "to-index" and "to-delete" JSON records
    end
```