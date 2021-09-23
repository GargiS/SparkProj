
The purpose is to generate RF (recency, frequency) metrics for page views for each user

Approach:
1. Read Input Files to DataFrame
2. Merge/Join Lookup csv file and add webPage_type to the inputDataFrame
3. Apply and generate each metric for each user in format user_id:metricName
4. Merge/Join all metrics generated at step3
5. Write the output to CSV file.

Deployment Steps:
1. Install sbt and run deploy.sh

Optimizations/Next Steps:

1. Strict Type checking: For stricter dqta type checking, define case class and verify
2. Partitioning Strategy:The output can be partitioned on dateofReference/pageType, depends on downstream requirement
3. Configuration for Output Files: The number of output/records per file can be changed as per requirements
4. Broadcast join: Based on the cluster configuration, for merging lookup file broadcast join can be used for improving performance.