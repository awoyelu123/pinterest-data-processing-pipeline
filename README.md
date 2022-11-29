# pinterest-data-processing-pipeline

## Project Overview

As the system overview diagram shown below, this project developed an end-to-end data processing pipeline in Python based on Pinterests experiment processing pipeline.It is implemented based on Lambda architecture to take advantage of both batch and stream-processing.

Firstly, Creating an API and using Kafka to distribute the data between S3 and Spark streaming.

# 2. Data Ingestion
## 2.1 Configuring the API

To emulate the live environment, the project design an API listening for events made by users on the app, or developer requests to the API. In the mean time, a user emulations script was created to simulate users uploading data to Pinterest.



