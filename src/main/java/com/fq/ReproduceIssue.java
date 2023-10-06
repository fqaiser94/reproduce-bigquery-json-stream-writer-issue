package com.fq;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStreamName;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReproduceIssue {

    static Map<String, String> ENV_VARIABLES = System.getenv();
    static String PREFIX = "REPRODUCE_ISSUE_";
    static String PROJECT_ID = ENV_VARIABLES.get(PREFIX + "PROJECT_ID");
    static String DATASET_ID = ENV_VARIABLES.get(PREFIX + "DATASET_ID");
    static String TABLE_ID = ENV_VARIABLES.get(PREFIX + "TABLE_ID");

    static TableReference TABLE_REFERENCE =
            new TableReference()
                    .setProjectId(PROJECT_ID)
                    .setDatasetId(DATASET_ID)
                    .setTableId(TABLE_ID);

    static String STRING_FIELD_NAME = "string_col";
    static String STRING_WITH_DEFAULT_VALUE_FIELD_NAME = "string_col_with_default_value";

    static TableSchema TABLE_SCHEMA = new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName(STRING_FIELD_NAME).setType("STRING").setMode("REQUIRED"),
            // notice how we've set a default value expression for this field so I should be able to send rows missing this field
            new TableFieldSchema().setName(STRING_WITH_DEFAULT_VALUE_FIELD_NAME).setType("STRING").setMode("REQUIRED").setDefaultValueExpression("'world'")
    ));

    static Table TABLE = new Table().setTableReference(TABLE_REFERENCE).setSchema(TABLE_SCHEMA);


    static void createTable() {
        try {
            Bigquery bigQuery = new Bigquery.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    GsonFactory.getDefaultInstance(),
                    new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()))
                    .build();
            try {
                bigQuery.tables().insert(PROJECT_ID, DATASET_ID, TABLE).execute();
            } catch (final GoogleJsonResponseException e) {
                // 409 = ALREADY_EXISTS
                if (e.getStatusCode() != 409) {
                    throw e;
                }
            }
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        try {
            createTable();

            BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient.create();
            JsonStreamWriter streamWriter =
                    JsonStreamWriter.newBuilder(
                                    WriteStreamName.of(PROJECT_ID, DATASET_ID, TABLE_ID, "_default").toString(),
                                    // explicitly specify schema specifically excluding STRING_WITH_DEFAULT_VALUE_FIELD_NAME
                                    com.google.cloud.bigquery.storage.v1.TableSchema.newBuilder()
                                            .addFields(com.google.cloud.bigquery.storage.v1.TableFieldSchema.newBuilder()
                                                    .setName(STRING_FIELD_NAME).setType(com.google.cloud.bigquery.storage.v1.TableFieldSchema.Type.STRING)
                                                    .setMode(com.google.cloud.bigquery.storage.v1.TableFieldSchema.Mode.REQUIRED)
                                                    .build())
                                            // .addFields(com.google.cloud.bigquery.storage.v1.TableFieldSchema.newBuilder()
                                            //         .setName(STRING_WITH_DEFAULT_VALUE_FIELD_NAME).setType(com.google.cloud.bigquery.storage.v1.TableFieldSchema.Type.STRING)
                                            //         .setMode(com.google.cloud.bigquery.storage.v1.TableFieldSchema.Mode.REQUIRED)
                                            //         .build())
                                            .build(),
                                    bigQueryWriteClient)
                            // I don't even need this anymore
                            // .setDefaultMissingValueInterpretation(AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)
                            .build();

            // This does work correctly now!
            streamWriter.append(new JSONArray().put(
                    new JSONObject().put(STRING_FIELD_NAME, "hello"))).get();

            // However, I can no longer send messages which have all the fields present. This throws an error now.
            streamWriter.append(new JSONArray().put(
                    new JSONObject().put(STRING_FIELD_NAME, "foo").put(STRING_WITH_DEFAULT_VALUE_FIELD_NAME, "bar"))).get();

            streamWriter.close();
            bigQueryWriteClient.close();
        } catch (DescriptorValidationException | InterruptedException | IOException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
