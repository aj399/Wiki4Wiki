# Wiki4Wiki
  My application discovers anomalies in page requests of Wikipedia articles , by juxtaposing 7 months of historical data with the current streaming data. Every minute it detects the articles that are trending in the last 1 hour among 6 million possible articles.  Historical spark batch job that averages hourly page requests of 10 million different articles over 7 months which is about 1 TB. In streaming side flink aggregates the page requests of 6 million different articles over a 1 hr period and that data is send to s3. And every hour a spark job identifies the articles with anomalous page requests pattern by juxtaposing the aggregated streaming data and the historical average


