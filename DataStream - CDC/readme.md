## Instructions

### Enable the Datastream API

From the Navigation menu, go to `Analytics > Datastream > Connection Profiles`. Click `Enable` to enable the Datastream API.

### Create Connection Profiles

You'll need to create two connection profiles: one for the PostgreSQL source, and another for the BigQuery destination.

#### PostgreSQL Connection Profile

1. In the Cloud console, navigate to the `Connection Profiles` tab and click `Create Profile`.
2. Select the `PostgreSQL` connection profile type.
3. Enter the Database connection details:

   - **IP:** 
   - **Port:**
   - **Username:**
   - **Password:** 
   - **Database:**

4. Click `Continue`.
5. Click `RUN TEST` to make sure that Datastream can reach the database.
6. Click `Create`.

#### BigQuery Connection Profile

1. In the Cloud console, navigate to the `Connection Profiles` tab and click `Create Profile`.
2. Select the `BigQuery` connection profile type.
3. Use `bigquery-cp` as the name and ID of the connection profile.
4. Click `Create`.

### Create Stream

1. In the Cloud console, navigate to the `Streams` tab and click `Create Stream`.
2. Define the stream details:
   - Enter a name and ID for the stream.
   - Select `PostgreSQL` as the source type.
   - Select `BigQuery` as the destination type.
3. Click `CONTINUE`.
4. Define & test source:
   - Select the PostgreSQL connection profile created in the previous step.
   - Test connectivity by clicking `RUN TEST`.
5. Click `CONTINUE`.
6. Configure the source:
   - Specify the replication slot name as `test_replication`.
   - Specify the publication name as `test_publication`.
   - Select the `test` schema for replication.
7. Click `Continue`.
8. Define the destination:
   - Select the BigQuery connection profile created in the previous step.
9. Click `Continue`.
10. Configure the destination:
   - Choose `us-central1` as the BigQuery dataset location.
   - Set the staleness limit to 0 seconds.
11. Click `Continue`.
12. Review and create the stream:
    - Validate the stream details by clicking `RUN VALIDATION`. 
    - Once validation completes successfully, click `CREATE AND START`.
    - Wait approximately 1-2 minutes until the stream status is shown as running.
