# Spark Jobs
## Local
1. Setup `gcloud init` [Doc](https://cloud.google.com/sdk/gcloud/reference/init)
2. Run `gcloud auth activate-service-account --key-file [KEY_FILE]`
3. Point `KEY_FILE` path in `Main.java`
4. Set program arguements in `IntelliJ` look at Main for example

## Build and deploy
1. Build JAR using
```shell script
./gradlew shadowJar
```
2. Upload JAR to `GCS` to our datalake bucket (ensure you have done `gcloud init`)
```shell script
gsutil cp build/libs/bdm-1.0-SNAPSHOT-all.jar gs://listery-datalake
```