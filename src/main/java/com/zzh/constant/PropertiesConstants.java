package com.zzh.constant;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-12 17:23
 **/
public class PropertiesConstants {
    public static final String AUTHOR = "zzh";
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_SINK_BROKERS = "kafka.sink.brokers";
    public static final String KAFKA_SINK_TOPIC = "kafka.sink.topic";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "zzh-group";
    public static final String METRICS_TOPIC = "metrics.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String PROPERTIES_FILE_NAME = "/application.properties";

    //es config
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
    public static final String ELASTICSEARCH_CLUSTER_NAME = "cluster.name";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";

    //mysql
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
}
