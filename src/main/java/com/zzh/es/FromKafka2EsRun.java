package com.zzh.es;

import com.zzh.domain.MetricEvent;
import com.zzh.utils.ElasticSearchSinkUtil;
import com.zzh.utils.ExecutionEnvUtil;
import com.zzh.utils.GsonUtil;
import com.zzh.utils.KafkaConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.zzh.constant.PropertiesConstants.*;
import static com.zzh.utils.ElasticSearchSinkUtil.addSink;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-12 16:55
 **/
@Slf4j
public class FromKafka2EsRun {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        //从kafka读取数据
        DataStreamSource<MetricEvent> source = KafkaConfigUtil.buildSource(env);
        List<InetSocketAddress> esAddresses = ElasticSearchSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        String clusterName = parameterTool.get(ELASTICSEARCH_CLUSTER_NAME, "zzh-es-cluster");
        Map<String, String> config = new HashMap<>();
        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(bulkSize));
        config.put(ELASTICSEARCH_CLUSTER_NAME, clusterName);
        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);
        int parallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);
        addSink(source, parallelism, config, esAddresses, (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> requestIndexer.add(Requests.indexRequest()
                .index(AUTHOR + "_" + metric.getName())
                .type(AUTHOR)
                .source(GsonUtil.toJSONBytes(metric), XContentType.JSON)));
        //source.print();
        env.execute("from kafka to es5 run");
    }
}
