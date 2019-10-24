package ambrose.ntk.bigquery.rest_and_bigquery.controllers;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

@RestController
public class MainController {

    BigQuery bigquery;

    // Get PROJECT_ID AND CREDENTIALS from Google Cloud Platform
    // Enable BigQuery API
    // Create new Project
    final String PROJECT_ID = "geo-project-x";

    // Create new Service Account keys (APIs & Services -> Credentials -> Create credentials (Service account key)
    // -> New Service account JSON (Role: Project -> Owner))
    final String CREDENTIAL_FILE_LOCATION = "D:\\Repos\\Java\\geo-project-x-be35cad1502b.json";

   public MainController(){
       super();
       try {
           bigquery = BigQueryOptions.newBuilder().setProjectId(PROJECT_ID).setCredentials(
                   ServiceAccountCredentials.fromStream(new FileInputStream(CREDENTIAL_FILE_LOCATION))
           ).build().getService();
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

    @GetMapping("/ping")
    String sayHello() {
        return "Hello World";
    }

    @GetMapping("/query/stackoverflow/popularposts")
    ArrayList<String> queryStackOverflow() {
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "SELECT "
                                + "CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, "
                                + "view_count "
                                + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
                                + "ORDER BY favorite_count DESC LIMIT 50")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        try {
            // Wait for the query to complete.
            queryJob = queryJob.waitFor();
            // Get the results.
            TableResult result = queryJob.getQueryResults();
            ArrayList<String> response = new ArrayList<>();
            for (FieldValueList row : result.iterateAll()) {
                response.add(row.get("url").getStringValue());
            }
            return response;
        }
        catch(InterruptedException e){
            e.printStackTrace();
        }
        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        return new ArrayList<>();
    }

}
