## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770 ✅
* 766,792
* 299,234
* 822,132


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *` ✅
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`


## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920 ✅
- 12,282,990
- 27,235,753
- 11,338,483



## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605 ✅
- 190,225



## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392` ✅


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8 ✅
- 10

<details open>
<summary>Answer</summary>
#### Answer 1. Load January 2020 data
```
$ python flows/02_gcp/etl_web_to_gcs.py 
22:45:47.787 | INFO    | prefect.engine - Created flow run 'attentive-goldfish' for flow 'etl-web-to-gcs'
22:45:48.363 | INFO    | Flow run 'attentive-goldfish' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
...
22:46:25.323 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
...  
22:46:36.240 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
22:46:36.407 | INFO    | Flow run 'attentive-goldfish' - Finished in state Completed('All states completed.')
```
#### Answer 2. Scheduling with Cron
```
$ prefect deployment build flows/02_gcp/etl_web_to_gcs.py:etl_web_to_gcs --cron "0 5 1 * *" -n "web_to_gcs_cron_homework" 
```
#### Answer 3. Loading data to BigQuery 
Link into [file](../../week_2_workflow_orchestration/flows/02_gcp/etl_gcs_to_bq.py)
```
$ python flows/02_gcp/etl_gcs_to_bq.py 
14:07:58.921 | INFO    | prefect.engine - Created flow run 'innocent-inchworm' for flow 'etl-parent-flow'
14:07:59.291 | INFO    | Flow run 'innocent-inchworm' - Created subflow run 'solemn-flamingo' for flow 'etl-gcs-to-bq'
...
14:11:29.988 | INFO    | Flow run 'solemn-flamingo' - Rows transfered: 7019375
14:11:30.094 | INFO    | Flow run 'solemn-flamingo' - Finished in state Completed()
14:11:30.287 | INFO    | Flow run 'innocent-inchworm' - Created subflow run 'statuesque-octopus' for flow 'etl-gcs-to-bq'
14:11:30.452 | INFO    | Flow run 'statuesque-octopus' - Created task run 'extract_from_gcs-968e3b65-0' for task 'extract_from_gcs'
...
14:15:49.847 | INFO    | Flow run 'statuesque-octopus' - Rows transfered: 7832545
14:15:49.914 | INFO    | Flow run 'statuesque-octopus' - Finished in state Completed()
14:15:49.916 | INFO    | Flow run 'innocent-inchworm' - Total data being transfered into bigQuery is: 14851920
14:15:49.975 | INFO    | Flow run 'innocent-inchworm' - Finished in state Completed('All states completed.')
```
#### Answer 4. Github Storage Block
I assume that what I do here is right, because when i try to build an flow from github like [this](https://github.com/PrefectHQ/prefect/pull/6598), it doesn't work.  
It keep says that the directory or file not found  
`Script at './week_2_workflow_orchestration/flows/02_gcp/etl_web_to_gcs.py' encountered an exceptio: FileNotFoundError(2, 'No such file or directory)`

So I try using [this](https://github.com/anna-geller/prefect-deployment-patterns/blob/main/blocks/storage_blocks/public_github_repository.py) method, and this is the files look like [here](../../week_2_workflow_orchestration/blocks/week_2_workflow_orchestration/flows/02_gcp/etl_web_to_gcs.py)  

My script:
```
$ prefect deployment build blocks/week_2_workflow_orchestration/flows/02_gcp/etl_web_to_gcs.py:etl_web_to_gcs -n "web_to_gcs_homework" --params='{"color":"green", "year":2020, "month":11}' -a
```

My output after run the flow:
```
22:48:59.408 | INFO    | prefect.agent - Submitting flow run 'a784a674-dbd1-4ca9-8a3a-f9263b705224'
22:48:59.949 | INFO    | prefect.infrastructure.process - Opening process 'slick-kakapo'... 
...
22:51:54.587 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605
22:51:54.648 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()        
...
22:51:57.762 | INFO    | Flow run 'slick-kakapo' - Finished in state Completed('All states completed.')
22:52:03.027 | INFO    | prefect.infrastructure.process - Process 'slick-kakapo' exited cleanly.
```
#### Answer 5. Email or Slack notifications
Notification look like:
```
Prefect <no-reply@prefect.io>
10.58 (8 menit yang lalu)
kepada saya

Flow run etl-web-to-gcs/sage-mule entered state `Completed` at 2023-02-02T03:58:26.800592+00:00.
Flow ID: 935bad3b-3aa2-4c15-b2ea-5385a76e7d91
Flow run ID: b0c2ea32-e763-4b88-b199-1b54256842cb
Flow run URL: https://app.prefect.cloud/account/d97c7cf3-034b-42e3-98b1-847c000e7c48/workspace/8671f19b-9a00-4fd1-b96a-7458eab45ead/flow-runs/flow-run/b0c2ea32-e763-4b88-b199-1b54256842cb
State message: All states completed.
```
Output:
```
10:57:23.320 | INFO    | prefect.agent - Submitting flow run 'b0c2ea32-e763-4b88-b199-1b54256842cb'
10:57:24.720 | INFO    | prefect.infrastructure.process - Opening process 'sage-mule'...
10:57:25.035 | INFO    | prefect.agent - Completed submission of flow run 'b0c2ea32-e763-4b88-b199-1b54256842cb'
...
10:58:13.515 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 514392
...
10:58:27.340 | INFO    | Flow run 'sage-mule' - Finished in state Completed('All states completed.')
10:58:32.290 | INFO    | prefect.infrastructure.process - Process 'sage-mule' exited cleanly.

```
</details>