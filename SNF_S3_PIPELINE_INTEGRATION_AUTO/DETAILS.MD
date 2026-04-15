1. Data Integration & Loading
	• External Stages: Allows you to link Snowflake directly to external cloud storage like AWS S3, Azure Blob Storage, or Google Cloud Storage 16:53 Opens in a new window .
	• Snowpipe: A serverless data ingestion service that enables real-time data loading as soon as files appear in a bucket 01:46 Opens in a new window .
	• Storage Integrations: An object that stores a generated IAM entity for your external cloud provider, allowing Snowflake to access files without sharing secret credentials 17:46 Opens in a new window .
	• Copy Into Command: The primary command for loading data, which includes advanced error handling like ON_ERROR = CONTINUE or SKIP_FILE 40:06 Opens in a new window .
2. Architecture & Performance
	• Separation of Storage and Compute: You pay for storage and compute separately. You can scale compute resources (Virtual Warehouses) up or down instantly without downtime.
	• Multi-Cluster Shared Data: Multiple users or departments can access the same data simultaneously without performance degradation.
	• Micro-Partitioning: Data is automatically partitioned into small, compressed units, which optimizes query performance without manual tuning.
3. Data Management & Protection
	• Time Travel: Enables you to query data as it existed at any point in the past (up to 90 days for Enterprise accounts), allowing for easy recovery from accidental deletes or updates.
	• Zero-Copy Cloning: Create a clone of a database, schema, or table instantly without duplicating the actual data storage. This is ideal for testing and development.
	• Fail-safe: A 7-day period after Time Travel expires where Snowflake can recover lost data in case of a disaster.
4. Table Types
Snowflake offers different table types to balance persistence and cost 03:36:53 Opens in a new window :
	• Permanent Tables: Default tables that include full Time Travel and Fail-safe features.
	• Temporary Tables: Tables that only exist for the duration of a session; they are wiped once the session ends 03:37:00 Opens in a new window .
	• Transient Tables: Tables that persist beyond a session but do not have a Fail-safe period, reducing storage costs for non-critical data.
5. Advanced Features
	• Data Sharing: Securely share data with other Snowflake accounts (or non-accounts via Reader Accounts) without moving or copying the data.
	• Marketplace: Access third-party datasets directly within your Snowflake environment.
	• Object Tagging: A governance feature used to track sensitive data and manage compliance across the platform.
	
	
	
	
	
	
	
	
	s3://snf-integration-demo/customers.csv
	
	To create the Arn for storage integration
	
	To get this ARN (Amazon Resource Name), you need to create an IAM Role inside your AWS Console. This role acts as the "identity" that Snowflake will assume to read your S3 files.
	Here is the step-by-step process to get that string:
	Step 1: Log in to AWS Console
		1. Go to the AWS Management Console.
		2. Search for IAM in the search bar and click on it.
	Step 2: Create the Role
		1. In the left sidebar, click Roles, then click Create role.
		2. Select trusted entity: Choose AWS account.
		3. An AWS account: Select This account (for now—you will update this later with Snowflake's specific ID).
		4. Click Next.
	Step 3: Attach Permissions
		1. Search for AmazonS3ReadOnlyAccess (or create a custom policy if you want to restrict it to just one bucket).
		2. Check the box next to the policy.
		3. Click Next.
	Step 4: Name and Create
		1. Role name: Give it a clear name, like snowflake_s3_readonly.
		2. Click Create role.
	Step 5: Copy the ARN
		1. Once created, click on the name of your new role (snowflake_s3_readonly) in the list.
		2. On the Summary page, you will see ARN at the top right.
		3. It will look exactly like this: arn:aws:iam::123456789012:role/snowflake_s3_readonly.
		4. Copy this string and paste it into your Snowflake CREATE STORAGE INTEGRATION command.
		
	666278775161
	
	From <https://us-east-1.console.aws.amazon.com/iam/home?region=ap-south-1#/roles/create> 
	
	
	Role creted 
	
	Arn- 
arn:aws:iam::666278775161:role/snowflake-s3-integration
	
	
	Method 1: The "One Pipe per Table" Strategy (Recommended)
	This is the standard approach. You organize your S3 bucket into subfolders (prefixes) and create a dedicated pipe for each "type" of data.
		• S3 Folder: s3://bucket/netflix/movies/ $\rightarrow$ Pipe: pipe_movies $\rightarrow$ Table: raw_movies
		• S3 Folder: s3://bucket/netflix/ratings/ $\rightarrow$ Pipe: pipe_ratings $\rightarrow$ Table: raw_ratings
	The Code Adjustment:
	When you create the pipe, you specify the subfolder in the FROM clause:
	SQL
	
	CREATE OR REPLACE PIPE movie_data_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO MOVIELENS.RAW.RAW_MOVIES
  FROM @my_s3_stage/movies/  -- <--- Specific Subfolder
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER 
	
	From <https://gemini.google.com/glic?hl=en> 
	
AZURE ADF = LAMBDA
