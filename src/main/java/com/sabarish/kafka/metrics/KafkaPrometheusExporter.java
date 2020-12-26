package com.sabarish.kafka.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;

/**
 * 
 * Nothing much to say here other than that you should check out the README
 * 
 * @author SSasidharan
 *
 */
public class KafkaPrometheusExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPrometheusExporter.class);

    private Gauge consumerGroupLagGauge = Gauge.build().name("kafka_consumergroup_lag").labelNames("consumergroup", "topic", "partition")
        .help("Consumer lag")
            .create().register();
    private Gauge consumerGroupOffsetsGauge = Gauge.build().name("kafka_consumergroup_current_offset")
        .help("Current consumer group offsets")
            .labelNames("consumergroup", "topic", "partition").create().register();
    private Gauge consumerGroupStateGauge = Gauge.build().name("kafka_consumergroup_state").labelNames("consumergroup")
        .help("State of consumer group").create().register();
    private Gauge consumerGroupMembersGauge = Gauge.build().name("kafka_consumergroup_members")
          .help("Number of active members in consumer group")
            .labelNames("consumergroup").create().register();
    // private Gauge consumerGroupActiveGauge =
    // Gauge.build().name("kafka_consumergroup_active").labelNames("topic",
    // "partition").create().register();
    // private Gauge consumerGroupRebalancingGauge =
    // Gauge.build().name("kafka_consumergroup_rebalancing").labelNames("topic",
    // "partition").create().register();
    // private Gauge topicOffsetsGauge =
    // Gauge.build().name("kafka_topic_partition_current_offset").labelNames("topic",
    // "partition").create().register();
    private Gauge topicUnderReplicatedPartitionsGauge = Gauge.build().name("kafka_topic_partition_under_replicated")
        .help("under replicated partitions")    
        .labelNames("topic", "partition").create().register();
    private Gauge topicNoLeaderGauge = Gauge.build().name("kafka_topic_partition_no_leader").labelNames("topic", "partition")
        .help("partitions leadership status").create()
        .register();
    private Gauge topicImbalanceGauge = Gauge.build().name("kafka_topic_leader_imbalance").labelNames("topic")
        .help("topic level imbalance").create().register();

    public void collect() throws IOException {
      LOGGER.info("Started");
      int metricsPort = Integer
          .valueOf(StringUtils.isEmpty(System.getenv("prometheus_metrics_port")) ? "9100" : System.getenv("prometheus_metrics_port"));
      HTTPServer metricsServer = new HTTPServer(metricsPort);
        String hostNameRegex = System.getenv("host_name_regex");
        boolean execute = true;
        if (StringUtils.isNotEmpty(hostNameRegex)) {
            String podName = System.getenv("HOSTNAME");
            execute = Pattern.matches(hostNameRegex, podName);
            LOGGER.info("Not going to generate metrics because execution is turned off with host_name_regex: {}", hostNameRegex);
        }
        if (execute) {
            int refreshIntervalMs = Integer
                    .valueOf(StringUtils.isEmpty(System.getenv("refresh_ms")) ? "20000" : System.getenv("refresh_ms"));
            String lowPriorityConsumerGroupsRegex = System.getenv("low_priority_group_regex");
            int lowPriorityConsumerGroupsRefreshIntervalMs = Integer
                    .valueOf(StringUtils.isEmpty(System.getenv("low_priority_group_refresh_ms")) ? "300000"
                            : System.getenv("low_priority_group_refresh_ms"));
            int listConsumerGroupsFrequencyMs = Integer
                    .valueOf(StringUtils.isEmpty(System.getenv("consumer_groups_list_refresh_ms")) ? "300000"
                            : System.getenv("consumer_groups_list_refresh_ms"));
            int nThreads = Integer.valueOf(StringUtils.isEmpty(System.getenv("number_threads")) ? "5" : System.getenv("number_threads"));
            String topicsExclusionRegex = System.getenv("topic_excl_regex");
            Properties props = initializeKafkaProperties();
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads);
            Map<String, String> env = System.getenv();
            Map<String, List<String>> ignoreTopicsPerCg = getTopicsToIgnorePerCg(env);
            long lastLowPriorityCgRefreshInstant = System.currentTimeMillis();
            Pattern lowPriorityCgPattern = null;
            if (StringUtils.isNotEmpty(lowPriorityConsumerGroupsRegex)) {
                lowPriorityCgPattern = Pattern.compile(lowPriorityConsumerGroupsRegex);
            }
            final Pattern topicsExclPattern = StringUtils.isNotEmpty(topicsExclusionRegex) ? Pattern.compile(topicsExclusionRegex) : null;
            long lastCgRefreshInstant = 0;
            Collection<ConsumerGroupListing> groups = null;
            List<String> topics = null;
            Vector<TopicPartition> topicPartitions = new Vector<>();
            try {
                while (true) {
                    long startMillis = System.currentTimeMillis();
                    try (AdminClient client = KafkaAdminClient.create(props);
                            KafkaConsumer<?, ?> consumer = new KafkaConsumer(props);) {
                        LOGGER.info("Active thread count in pool: {}", executor.getPoolSize());
                        if ((groups == null) || (System.currentTimeMillis() - lastCgRefreshInstant > listConsumerGroupsFrequencyMs)) {
                            ListConsumerGroupsResult groupsResult = client.listConsumerGroups();
                            try {
                                groups = groupsResult.all().get();
                            } catch (ExecutionException ee) {
                                LOGGER.error("Exception when listing consumer groups", ee);
                            }
                            if (topicPartitions.isEmpty()) {
                                topicPartitions = getTopicPartitions(client, executor, groups);
                            }
                            ListTopicsResult listTopics = client.listTopics();
                            try {
                                topics = listTopics.listings().get().stream()
                                        .filter(t -> !t.isInternal())
                                        .map(t -> t.name())
                                        .filter(n -> topicsExclPattern != null ? !topicsExclPattern.matcher(n).matches() : true)
                                        .collect(Collectors.toList());
                            } catch (ExecutionException ee) {
                                LOGGER.error("Exception when listing topics", ee);
                            }
                        }
                        LOGGER.info("Total number of consumer groups: {}", groups.size());
                        LOGGER.info("Total number of topics: {}", topics.size());
                        LOGGER.info("Total number of topic partitions used by consumer groups: {}", topicPartitions.size());
                        updateTopicGauges(client, executor, topics);
                        final ConcurrentHashMap<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>(
                                consumer.endOffsets(topicPartitions));
                        LOGGER.info("Total number groups: {}", groups.size());
                        List<Callable<Integer>> callables = new ArrayList<>();
                        for (ConsumerGroupListing group : groups) {
                            boolean lowPriorityCg = lowPriorityCgPattern != null && lowPriorityCgPattern.matcher(group.groupId()).matches();
                            boolean refresh = lowPriorityCg
                                    ? System.currentTimeMillis()
                                            - lastLowPriorityCgRefreshInstant > lowPriorityConsumerGroupsRefreshIntervalMs
                                    : true;
                            if (refresh) {
                                callables.add(() -> {
                                    LOGGER.info("Processing metrics for {}", group.groupId());
                                    try {
                                        updateConsumerGroupOffsetGauges(client, consumer, group, ignoreTopicsPerCg, endOffsets);
                                    } catch (ExecutionException ee) {
                                        LOGGER.error("Exception when fetching offsets for consumer group", ee);
                                        return 1;
                                    } catch (InterruptedException e1) {
                                        LOGGER.error("Interrupted when waiting on offsets. Returning immediately.");
                                        Thread.currentThread().interrupt();
                                        return 1;
                                    }
                                    try {
                                        updateConsumerGroupStateGauges(client, group);
                                    } catch (ExecutionException ee) {
                                        LOGGER.error("Exception when describing consumer groups", ee);
                                        return 1;
                                    } catch (InterruptedException e) {
                                        LOGGER.error("Interrupted when waiting on member descriptions. Returning immediately.");
                                        Thread.currentThread().interrupt();
                                        return 1;
                                    }
                                    return 0;
                                });
                                if (lowPriorityCg) {
                                    lastLowPriorityCgRefreshInstant = System.currentTimeMillis();
                                }
                            }
                        }
                        executor.invokeAll(callables);
                    }
                    LOGGER.info("Finished gathering metrics in {} ms. Sleeping for {}", System.currentTimeMillis() - startMillis,
                            refreshIntervalMs);
                    Thread.sleep(refreshIntervalMs);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted when waiting. Returning immediately.");
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void updateConsumerGroupOffsetGauges(AdminClient client, KafkaConsumer<?, ?> consumer, ConsumerGroupListing group,
            Map<String, List<String>> ignoreTopicsPerCg, final ConcurrentHashMap<TopicPartition, Long> endOffsets)
            throws InterruptedException, ExecutionException {
        ListConsumerGroupOffsetsResult listConsumerGroupOffsets = client.listConsumerGroupOffsets(group.groupId());
        final List<String> ignoreTopics = ignoreTopicsPerCg.containsKey(group.groupId()) ? ignoreTopicsPerCg.get(group.groupId())
                : Collections.EMPTY_LIST;
        Map<TopicPartition, OffsetAndMetadata> offsetsMap = listConsumerGroupOffsets.partitionsToOffsetAndMetadata().get();
        offsetsMap.entrySet().stream().filter(e -> !ignoreTopics.contains(e.getKey().topic())).forEach(e -> {
            // because we are refreshing consumer group definitions
            // at a different frequency, we may be missing some
            // topic partitions of that cg and hence the
            // end offsets for those topic partitions
            if (!endOffsets.containsKey(e.getKey())) {
                // kafka consumer is not threadsafe
                synchronized (this) {
                    Collection<Long> offsets = consumer.endOffsets(Arrays.asList(e.getKey())).values();
                    if (!offsets.isEmpty()) {
                        endOffsets.putIfAbsent(e.getKey(), offsets.iterator().next());
                    }
                }
            }
            consumerGroupLagGauge.labels(group.groupId(), e.getKey().topic(), String.valueOf(e.getKey().partition()))
                    .set(endOffsets.get(e.getKey()) - e.getValue().offset());
            consumerGroupOffsetsGauge.labels(group.groupId(), e.getKey().topic(), String.valueOf(e.getKey().partition()))
                    .set(e.getValue().offset());
        });
    }

    private void updateConsumerGroupStateGauges(AdminClient client, ConsumerGroupListing group)
            throws InterruptedException, ExecutionException {
        DescribeConsumerGroupsResult describeGroups = client.describeConsumerGroups(Arrays.asList(group.groupId()));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futures = describeGroups.all();
        Map<String, ConsumerGroupDescription> descriptions = futures.get();
        for (Map.Entry<String, ConsumerGroupDescription> description : descriptions.entrySet()) {
            int state = convertConsumerStateToInt(description.getValue().state());
            consumerGroupStateGauge.labels(description.getValue().groupId()).set(state);
            consumerGroupMembersGauge.labels(description.getValue().groupId()).set(description.getValue().members().size());
        }
    }

    private void updateTopicGauges(AdminClient client, ThreadPoolExecutor executor, List<String> topics) throws InterruptedException {
        List<Callable<Integer>> callables = new ArrayList<>();
        List<String> fetchTopics = new ArrayList<>();
        for (Iterator<String> iter = topics.iterator(); iter.hasNext();) {
            String topic = iter.next();
            fetchTopics.add(topic);
            if ((fetchTopics.size() == 20) || !iter.hasNext()) {
                callables.add(() -> {
                    DescribeTopicsResult describeTopics = client.describeTopics(new ArrayList<String>(fetchTopics));
                    Collection<KafkaFuture<TopicDescription>> values = describeTopics.values().values();
                    for (KafkaFuture<TopicDescription> v : values) {
                        try {
                            TopicDescription td = v.get();
                            Map<Integer, Integer> leaderships = new HashMap<>();
                            //TODO Get min.isr from topic definition
                            td.partitions().stream().forEach(tp -> {
                                topicUnderReplicatedPartitionsGauge.labels(td.name(), String.valueOf(tp.partition()))
                                        .set(tp.isr().size() < 2 ? 1 : 0);
                                topicNoLeaderGauge.labels(td.name(), String.valueOf(tp.partition())).set(tp.leader() == null ? 1 : 0);
                                if (tp.leader() != null) {
                                    leaderships.put(tp.leader().id(), 1);
                                }
                            });
                            // TODO: Replace 3 by replication factor of topic
                            int even = td.partitions().size() / 3;
                            boolean imbalance = leaderships.entrySet().stream().anyMatch(e -> e.getValue() < even);
                            topicImbalanceGauge.labels(td.name()).set(imbalance ? 1 : 0);
                        } catch (ExecutionException ee) {
                            LOGGER.error("Exception when fetching topic description", ee);
                            return 1;
                        } catch (InterruptedException e1) {
                            LOGGER.error("Interrupted when fetching topic description. Returning immediately.");
                            Thread.currentThread().interrupt();
                            return 1;
                        }
                    }
                    return 0;
                });
                fetchTopics.clear();
            }
        }
        executor.invokeAll(callables);
    }

    private Vector<TopicPartition> getTopicPartitions(AdminClient client, ExecutorService executor, Collection<ConsumerGroupListing> groups)
            throws InterruptedException {
        ConcurrentHashMap<TopicPartition, TopicPartition> topicPartitions = new ConcurrentHashMap<>();
        List<Callable<Integer>> callables = new ArrayList<>();
        final List<String> fetchGroups = new ArrayList<>();
        for (Iterator<ConsumerGroupListing> iter = groups.iterator(); iter.hasNext();) {
            ConsumerGroupListing group = iter.next();
            fetchGroups.add(group.groupId());
            if ((fetchGroups.size() == 20) || !iter.hasNext()) {
                callables.add(() -> {
                    DescribeConsumerGroupsResult describeGroups = client.describeConsumerGroups(new ArrayList<String>(fetchGroups));
                    KafkaFuture<Map<String, ConsumerGroupDescription>> futures = describeGroups.all();
                    try {
                        Map<String, ConsumerGroupDescription> descriptions = futures.get();
                        for (Map.Entry<String, ConsumerGroupDescription> description : descriptions.entrySet()) {
                            description.getValue().members().forEach(
                                    md -> md.assignment().topicPartitions().stream().forEach(tp -> topicPartitions.putIfAbsent(tp, tp)));
                        }
                    } catch (ExecutionException ee) {
                        LOGGER.error("Exception when describing consumer groups", ee);
                        return 1;
                    } catch (InterruptedException e) {
                        LOGGER.error("Interrupted when waiting on member descriptions. Returning immediately.");
                        Thread.currentThread().interrupt();
                        return 1;
                    }
                    return 0;
                });
                executor.invokeAll(callables);
                callables.clear();
                fetchGroups.clear();
            }
        }
        return new Vector<TopicPartition>(topicPartitions.values());
    }

    private int convertConsumerStateToInt(ConsumerGroupState state) {
        if (state == ConsumerGroupState.STABLE) {
            return 1;
        }
        else if (state == ConsumerGroupState.PREPARING_REBALANCE) {
            return 3;
        }
        else if (state == ConsumerGroupState.COMPLETING_REBALANCE) {
            return 2;
        }
        else if (state == ConsumerGroupState.DEAD) {
            return 4;
        }
        else if (state == ConsumerGroupState.EMPTY) {
            return 5;
        }
        else {
            return 0;
        }
    }

    private Properties initializeKafkaProperties() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                StringUtils.isEmpty(System.getenv("bootstrap_server")) ? "kafka-0.kafka-svc:9093" : System.getenv("bootstrap_server"));
        props.setProperty(AdminClientConfig.CLIENT_ID_CONFIG,
                StringUtils.isEmpty(System.getenv("client_id")) ? "ignite_kafka_exporter" : System.getenv("client_id"));
        props.setProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG,
                StringUtils.isEmpty(System.getenv("metadata_max_age")) ? "300000" : System.getenv("metadata_max_age"));
        props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                StringUtils.isEmpty(System.getenv("request_timeout_ms")) ? "180000" : System.getenv("request_timeout_ms"));
        props.setProperty(AdminClientConfig.RETRIES_CONFIG, StringUtils.isEmpty(System.getenv("retries")) ? "3" : System.getenv("retries"));
        props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
                StringUtils.isEmpty(System.getenv("protocol")) ? AdminClientConfig.DEFAULT_SECURITY_PROTOCOL : System.getenv("protocol"));
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private Map<String, List<String>> getTopicsToIgnorePerCg(Map<String, String> env) {
        Map<String, List<String>> topicsToIgnorePerCg = new HashMap<>();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("ignore_topics_")) {
                String cgName = key.substring(14);
                String[] topics = entry.getValue().split(",");
                topicsToIgnorePerCg.put(cgName, Arrays.asList(topics));
            }
        }
        return topicsToIgnorePerCg;
    }
    
    public static void main(String[] args) {
      KafkaPrometheusExporter exporter = new KafkaPrometheusExporter();
      try {
        exporter.collect();
      }
      catch (Exception e) {
        LOGGER.info("Exception when collecting metrics", e);
      }
    }

}
