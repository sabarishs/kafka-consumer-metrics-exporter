# kafka-consumer-metrics-exporter

This is a simple humble exporter for Kafka consumer group and topic metrics. It connects to kafka and gathers the metrics for the different consumer groups and also includes a few key topic metrics related like under replicated partitions and leadership imbalance. It aims to help **optimize the load on kafka** when performing this kind of metrics collection. For ex, you can specify a different refresh interval for low priority consumer groups. Another example is that discovery of consumer groups by listing all of them does not need to be performed every time, it is controllable with another refresh interval. And another feature different from other exporters is that you can configure to exclude certain topics being reported for certain consumer groups. If you have been disappointed with lack of --delete-offsets in your version of Kafka, then you would understand why you need this feature.

# Metrics
- kafka_consumergroup_lag ("consumergroup", "topic", "partition")
  - Current lag for consumer groups
- kafka_consumergroup_current_offset ("consumergroup", "topic", "partition")
  - Current offset of consumer groups
- kafka_consumergroup_state ("consumergroup")
  - Current consumer group state encoded as the value (STABLE=1, PREPARING_REBALANCE=3, COMPLETING_REBALANCE=2, DEAD=4, EMPTY=5)
- kafka_consumergroup_members ("consumergroup", "topic", "partition")
  - Number of members in a consumer group
  - Use this to know if you have enough parallelism as you have configured
- kafka_topic_partition_under_replicated ("topic", "partition")
  - if number of in sync replicas is less than configured
- kafka_topic_partition_no_leader ("topic", "partition")
  - whether each partition has a leader or not
  - value is 0 if leader exists else 1
- kafka_topic_leader_imbalance ("topic")
  - 0 or 1 depending on if topic has an imbalance in leader to partition distribution or not

# Levers
Levers, levers, a lot many levers. We all love our switches, don't we? The following env vars are supported. If you hate env, jump to later part of the document.

- host_name_regex: specify a host name pattern. The exporter will collect metrics only if the host name in which it is running matches this regex. So you can collect metrics in only one kafka pod (or may be 2) but not in all your pods as it is a waste of compute and strain on kafka if we collect all metrics in all pods. If undefined in env, then exporter collects in all pods
- low_priority_group_regex: specify regex pattern for low priority consumer group names that will be fetched at a lower frequency from kafka
- low_priority_group_refresh_ms: specify the frequency for low priority consumer groups
- topic_excl_regex: specify the topics you want excluded
- ignore_topics_* : This is an interesting lever. If you are running a version of Kafka that does not support delete-offsets, this would be very useful. You can specify which topics should be ignored when reporting lag. You can specify one property for each consumer group that you want to handle this way. For ex ignore_topics_index_group=topic1,topic2 in env means lag calculate will exclude topics topic1, topic2 for group named index_group.
- consumer_groups_list_refresh_ms: Listing consumer groups every time can affect kafka performance. This lets you control how often you want to refresh the list of consumer groups. Default value is 300000 ie 5 mins.
- refresh_ms: Refresh interval for all information unless overridden with other properties. Default value is 20000.
- number_threads: Number of threads to use for parallelism when determining lag for consumer groups. Useful if number of consumer groups is high. Default is 5.
- bootstrap_server: specify the bootstrap url when using admin client. Default is kafka-0.kafka-svc:9093
- client_id: specify the client id to use when using the admin client. Default is ignite_kafka_exporter
- metadata_max_age: Default is 300000
- request_timeout_ms: Default is 180000
- retries: Default is 3.
- protocol: Default is PLAINTEXT

Or if you **hate env**, you can specify just one env to rule it all

- conf: fully qualified name to a properties file that has all the properties mentioned above

Or if you **really really hate env**, you can skip conf env variable as well. But make sure you place an application.properties file in /opt/kafka_exporter directory. You can use a kubernetes config map to do that. Look ma, no env!
