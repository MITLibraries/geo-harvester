# Alma GIS Harvests

The following are sequence diagrams related to harvesting of GIS data from Alma MARC records.

## Records added or modified in Alma invoking TIMDEX
```mermaid
sequenceDiagram
    participant gis_team as GIS Team
    participant alma_staff as Alma Staff
    participant alma as Alma
    participant s3_sftp as S3 SFTP
    participant webhook as Alma Webhook Lambda
    participant timdex_sf as TIMDEX StepFunction
    
    gis_team->>alma_staff: Create or help<br>identify GIS records
    alma_staff->>alma: Create or modify records
    alma->>s3_sftp: Daily and Full Exports<br>write records
    alma->>webhook: Send TIMDEX<br>Export Confirmation
    webhook->>timdex_sf: Invoke for TIMDEX<br>"alma" source 
    note right of timdex_sf: Pre-existing "alma" invocation
    webhook->>timdex_sf: Invoke for TIMDEX<br>"gisalma" source
    note right of timdex_sf: New "gisalma" invocation
```

## GeoHarvester fetching and filtering Alma MARC records for `gisalma` source

```mermaid
sequenceDiagram
    participant webhook as Alma Webhook Lambda
    participant s3alma as TIMDEX s3 "alma" folder
    participant gh as GeoHarvester
    participant s3gisalma as TIMDEX s3 "gisalma" folder
    participant geopipeline as Geo Records Pipeline
    
    webhook->>gh: Invoke "alma" harvest via CLI
    gh->>s3alma: Identify records for scanning
    s3alma-->>gh: Iterator of records
    loop For each MARC record
    	alt Matches geospatial filtering
    		gh->>gh: Transform to MITAardvark
    		gh->>s3gisalma: Write
				else Does not match geospatial filtering
				note right of gh: Skip
    	end
    end
    gh->>geopipeline: Follows same pipeline as<br>"gismit" and "gisogm" from here
```
