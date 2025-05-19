# Task 2 SD

### Exercice 1
- 1 queue SQS, with filter_text_worker as lambda trigger
- Results are saved to DynamoDB 
- get_worker_results first 100 results from DynamoDB via configured API gateway

### Exercice 2
- 1 queue SQS, without triggers
- stream check SQS queue metrics, and invoke functions, to process messages 

### Exercice 3
- In pyrun we create workspace, and add lithops_filter
- lithops_filter, gets files from s3 and process with map_function.
- finally map_function returns number of insults censored and reduce function sum its

### Exercice 4
- In pyrun we create workspace, and add lithops_filter
- lithops_filter, gets files from s3 and process with map_function.
- finally map_function returns number of insults censored and reduce function sum its
