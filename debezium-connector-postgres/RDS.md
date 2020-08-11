In order to run the test suite against an Amazon RDS PostgreSQL environment, there are several important steps that must be done in order for Debezium features to operate correctly on RDS and to allow remote connectivity to your Amazon Virtual Private Cloud (VPC) environment.

## Setup remote connectivity

When you setup an account with Amazon RDS, they should automatically create a new Security Group for your VPC environment.
By default this new security group does not allow remote connections from your local machine to the resources which it secures, so you you need to explicitly grant remote access.

1. Navigate to **VPC** > **Security** > **Security Groups**
2. Select your Security Group in the top pane
3. Select the **Inbound Rules** tab in the bottom pane
4. Select **Edit Rules**
5. Add a new Rule
    * Type: PostgreSQL
    * Protocol: TCP
    * Port Range: 5432
    * Source: 0.0.0.0/0 (for allowing access from any IP address), or a specific IP, host (range)
    * Description: Anything (optional)
6. Save rules

At this point your Security Group should now permit remote connections to any PostgreSQL instance secured by this security group.

## Setup a database parameter group

By default, Amazon RDS provides a set of parameter group configurations for basic database operations.
Unfortunately these do not enable all the necessary features that are needed for Debezium to perform its change data capture operations, most notably logical replication.
If you have already created a custom parameter group for your version of PostgreSQL, then you can skip this section.
If you have not, this will walk you through how to copy and customize the default configuration groups for our needs.

1. Navigate to **RDS**, this should take you to the RDS Dashboard.
2. Click **Parameter Groups** in the **Resources** panel on the dashboard.
3. Click **Create parameter group** button in the top right.
4. Configure your parameter group
    * Parameter group family: postgresXX.YY (where XX.YY is closest to your major version)
    * Group name: custom-postgresXX.YY
    * Description: Custom parameter group for postgres XX.YY

At this point all we've done is clone the default parameter group for a given version of PostgreSQL into our own.
We still need to make parameter modifications to support the features we need in order for Debezium to work properly.
You should be able to click on the name of your parameter group and it should take you to a tabular screen where each parameter is listed, its current configured value, a description of the parameter and other pertinent information.
At the top of this tabular layout is a search box we're going to use to quickly locate and edit parameters.

Click the `Edit parameters` button in the top right and then click in the search box.

##### Required Parameters
1. Search for `rds.logical_replication` and set its value to `1`.

##### Optional Parameters

1. Search for `log_replication_commands` and set its value to `1`.
2. Search for `log_statement` and set its value to `all`.

Once you've made these changes click `Save changes` to persist the parameter changes to your custom parameter group.

## Setup database instance

The latest RDS piece of setup we need to do is to actually install our desired PostgreSQL instance.

1. Navigate to **RDS**, this should take you to the RDS Dashboard.
2. Click **DB Instances** in the **Resources** panel on the dashboard.
3. Click **Create database**.

This is where all the _magic_ happens where we put tie together all the pieces of the prior configuration/setup we've done and how that interacts with our new RDS PostgreSQL environment.

1. Click the radio button to select **PostgreSQL**
2. Select the **Version** you wish to install.
Remember this version should match the version of database parameters you cloned in the prior steps.
3. Select the **Template** of interest, I typically picked **Free tier** because it's free ;)
4. Provide your database with a **DB Instance Identifier**.
5. Under **Credentials Settings**, manually assign a **RDS Master Password** and confirm it.
6. Under **Connectivity**, you'll want to set that it is _publicly accessible_.
7. You should be able to leave the **VPC security group** to use an existing and it should be set to **default** or whatever you named it to.
In the event you have multiple security groups and you created a new one, select the one of interest to assign.
In short it should be the security group that grants access to port 5432.

Feel free to customize any other database configuration options you want but they're entirely optional and defaults are perfectly acceptable.
Once you are satisfied with your configuration, click **Create database** and wait a few minutes while your instance is configured, built, and brought online for you.

## Connecting to Amazon RDS

In order to connect to your Amazon RDS PostgreSQL instance, you'll need to use the fully qualified endpoint name.
In order to determine your endpoint:

1. Navigate to **RDS**, this should take you to the RDS Dashboard.
2. Click **DB Instances** in the **Resources** panel on the dashboard.
3. Click on the designed **DB Identifier** for the instance you're interested in connecting to.
4. You should have a **Connectivity & security** tab where you can see the **Endpoint** name.

When you run your JVM process you either need to provide these VM options manually, provide them as a part of the exported environment or manually modify `TestHelper#defaultJdbcConfig()` specifying these values directly before running the tests.


```
database.dbname=postgres
database.hostname=<your amazon instance endpoint>
database.user=<name of RDS master user, by default this is postgres>
database.password=<your RDS master password>
```

## Viewing logs

If you're interested in watching the logs of your PostgreSQL instance if you configured the optional database parameters.

1. Navigate to **RDS**, this should take you to the RDS Dashboard.
2. Click **DB Instances** in the **Resources** panel on the dashboard.
3. Click on the designed **DB Identifier** for the instance you're interested in connecting to.
4. You should have a **Logs & events** tab where you can then scroll down to the **Logs** section.
5. Select the latest log of interest in the table and click **Watch**.
6. This takes you to a screen that updates periodically with the latest log entries.
