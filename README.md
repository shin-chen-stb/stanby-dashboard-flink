# stanby-dashboard-flink
Project by DPG

## What is this
- Flink application running on Kinesis Analytics Flink cluster
- Reading the stream in Kinesis Stream and write to Elasticsearch
- Find the realtime dashboard on Kibana

## How to build
```aidl
mvn clean package
```

## How to deploy
- Find your packaged jar file from /target directory
- Upload the jar file to S3 manually
  - `s3://aws-glue-scripts-943485981463-ap-northeast-1/dataplatform/applicationJars/`
- Find your kinesis analytics application and change the jar file path
  - You don't need to stop the app. Kinesis analytics would restart with snapshot after the setting changes.
