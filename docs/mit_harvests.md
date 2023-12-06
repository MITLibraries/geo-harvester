# MIT Harvests

The following are sequence diagrams related to harvesting of MIT GIS data.

## GIS team adds, modifies, or deletes files in S3 Upload bucket
```mermaid
sequenceDiagram
    participant gis_team as GIS Team
    participant s3_upload as S3:Upload
    participant geo_data_sf as Geo Data Handler <br> StepFunction
    participant s3_cdn_rest as S3:CDN:Restricted/MIT
    participant sqs_queue as GIS SQS Queue
    
    Note over gis_team, s3_upload: Zip file is added, <br>modified, or deleted
    gis_team->>s3_upload: Modify File
    Note over s3_upload, geo_data_sf: EventBridge trigger
    activate geo_data_sf
    s3_upload->>geo_data_sf: Invoke StateMachine
    geo_data_sf->>s3_cdn_rest: Copies or deletes zip file
    Note over geo_data_sf,sqs_queue: Message contains minimum of S3 paths <br> and event type (e.g. added, deleted) 
    geo_data_sf->>sqs_queue: Publish SQS message
    deactivate geo_data_sf
```

## Incremental Harvest 

The incremental harvest will process any messages currently in an SQS queue that indicate modifications were made to the `S3:CDN:Restricted` bucket.  After normalization, if records are determined to be public, it will send EventBridge events to trigger a StepFunction to copy from `S3:CDN:Restricted` to `S3:CDN:Public`.

```mermaid
sequenceDiagram
    autonumber
    participant sqs_queue as GIS SQS Queue
    participant s3_cdn_rest as S3:CDN:Restricted/MIT
    participant geo_harv as Geo Harvester
    participant s3_cdn_pub as S3:CDN:Public/MIT
    participant eb as EventBridge
    participant geo_data_sf as Geo Data Handler <br> StepFunction
    participant s3_timdex as S3:TIMDEX
    
    activate geo_harv
    geo_harv->>sqs_queue: Fetch messages
    sqs_queue-->>geo_harv: Messages
    loop SQS Messages of Modified Files
        geo_harv->>geo_harv: Validate SQS Message
        alt Message Invalid
            geo_harv->>sqs_queue: Log to Sentry and leave message
        end
        geo_harv->>geo_harv: Determine identifier from SQS Message
        geo_harv->>s3_cdn_rest: Read metadata file from zip file
        s3_cdn_rest-->>geo_harv: Source metadata        
        geo_harv->>geo_harv: Normalize source metadata to Aardvark
        
        alt Action: Created or Modified
            geo_harv->>s3_cdn_pub: WRITE source and MIT aardvark <br> metadata (may overwrite)        
            geo_harv->>eb: Send event noting restricted=true|false
            eb->>geo_data_sf: Async invoke
        else Action: Deleted
            geo_harv->>s3_cdn_pub: DELETE source and MIT aardvark <br> metadata
            geo_harv->>eb: Send event noting deleted=true
            eb->>geo_data_sf: Async invoke
        end
        geo_harv->>s3_timdex: Write MIT aardvark
    end
    deactivate geo_harv
```

## Full Harvest

A full harvest will use the `CDN:Restricted` files as the canonical source, reprocessing all zip files.

```mermaid
sequenceDiagram
    autonumber
    participant s3_cdn_rest as S3:CDN:Restricted/MIT
    participant sqs_queue as GIS SQS Queue
    participant geo_harv as Geo Harvester   
    participant s3_cdn_pub as S3:CDN:Public/MIT
    participant sqs_msg_loop as Loop Logic for SQS Messages
    
    activate geo_harv
    geo_harv->>sqs_queue: Fetch messages
    sqs_queue-->>geo_harv: Messages
    alt SQS queue is NOT empty
        break
            geo_harv->>geo_harv: Abort and log failure
        end
    else If SQS queue IS empty
        geo_harv->>s3_cdn_rest: List bucket/path objects
        s3_cdn_rest-->>geo_harv: List of filenames
        
        loop List of Zip files from Restricted
            Note over geo_harv,sqs_msg_loop: Treat ALL files like "Event:Created"
            geo_harv->>sqs_msg_loop: Prepare data packages for same logic loop<br>as processing SQS messages
        end
    end
    deactivate geo_harv
```