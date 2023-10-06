This repo reproduces the issue I'm facing where I can't write rows to BigQuery
that are missing fields that have a Default Value Expression defined in the TableSchema
even though I think I have correctly configured the StreamWriter using the newly released
`.setDefaultMissingValueInterpretation(AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)` option. 

You can execute the program by running:  

```bash
mvn clean package

export REPRODUCE_ISSUE_PROJECT_ID=xxx
export REPRODUCE_ISSUE_DATASET_ID=xxx
export REPRODUCE_ISSUE_TABLE_ID=xxx

java -cp target/reproduce-issue-1.0-SNAPSHOT.jar com.fq.ReproduceIssue
```

Example output:
```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
Oct. 05, 2023 9:07:28 P.M. com.google.cloud.bigquery.storage.v1.ConnectionWorker resetConnection
INFO: Start connecting stream: projects/flink-bq-writer/datasets/temp/tables/temp2/streams/_default id: 9a0afa7f-14a0-471b-a0c2-1e29be26dd03
Oct. 05, 2023 9:07:28 P.M. com.google.cloud.bigquery.storage.v1.ConnectionWorker resetConnection
INFO: Finish connecting stream: projects/flink-bq-writer/datasets/temp/tables/temp2/streams/_default id: 9a0afa7f-14a0-471b-a0c2-1e29be26dd03
Exception in thread "main" com.google.cloud.bigquery.storage.v1.Exceptions$AppendSerializationError: INVALID_ARGUMENT: Append serialization failed for writer: projects/flink-bq-writer/datasets/temp/tables/temp2/streams/_default
	at com.google.cloud.bigquery.storage.v1.SchemaAwareStreamWriter.append(SchemaAwareStreamWriter.java:210)
	at com.google.cloud.bigquery.storage.v1.SchemaAwareStreamWriter.append(SchemaAwareStreamWriter.java:123)
	at com.google.cloud.bigquery.storage.v1.JsonStreamWriter.append(JsonStreamWriter.java:62)
	at com.fq.ReproduceIssue.main(ReproduceIssue.java:92)
```