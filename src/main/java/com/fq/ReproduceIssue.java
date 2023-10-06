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
                                    bigQueryWriteClient)
                            // I've made sure to setDefaultMissingValueInterpretation correctly AFAICT
                            .setDefaultMissingValueInterpretation(AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)
                            .build();

            // Able to successfully send a JSON object that has all the fields defined in TABLE_SCHEMA
            streamWriter.append(new JSONArray().put(
                    new JSONObject().put(STRING_FIELD_NAME, "foo").put(STRING_WITH_DEFAULT_VALUE_FIELD_NAME, "bar"))).get();

            // Done this as well even though it should not be necessary since I've already configured the default behaviour on line 83
            streamWriter.setMissingValueInterpretationMap(Collections.singletonMap(
                    STRING_WITH_DEFAULT_VALUE_FIELD_NAME,
                    AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE));

            // Throws an error if I try to send a JSON object that is missing a field even though it has a DefaultValueExpression defined
            // I don't understand why this doesn't work. 
            streamWriter.append(new JSONArray().put(
                    new JSONObject().put(STRING_FIELD_NAME, "hello"))).get();

            streamWriter.close();
            bigQueryWriteClient.close();
        } catch (DescriptorValidationException | InterruptedException | IOException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
