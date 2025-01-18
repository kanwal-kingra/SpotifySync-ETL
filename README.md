# SpotifySync-ETL
For this project, the data was collected from Spotify using Spotify’s API and ‘spotipy’ library. The data is on Best Hindi songs 2023 in United States (ranked weekly)

https://open.spotify.com/playlist/0zc6Hq9OIAengtGG6a3lfs

**Services Used**: AWS Cloud - AWS Lambda, AWS Glue(SparkContext), AWS S3, AWS IAM, AWS admin, Snowflake, Spotify API

**ETL PIPELINE**:

- Signed Up to Spotify developer account to generate API keys.
- Assigned roles and policies for the services to be used using AWS IAM.
- Used Python code in AWS Lambda function to extract raw data using Spotify API and ‘spotipy’ python library.
- Loaded the extracted data in json format to AWS S3(raw_data/album , raw_data/song, raw_data/artist) using boto3.
- Executed transform function to convert the raw data into 3 csv files – album, artist, songs.
- Stored the transformed data into AWS S3 in csv format (transformed_data/album, transformed_data/song, transformed_data/artist)
- Designed Snowflake data model for tables and pipes to load data
- Created storage integration in Snowflake to access the data to S3 (external stage)
- Created snowpipe by leveraging sqs for automatically retrieving the data when new files land into in raw_data/album, raw_data/song, raw_data/artist directories
- Queried the snowflake tables
- Designed Views on top of Snowflake Tables to get insights into popularity of Artists and their songs and rank them.
