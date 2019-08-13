package com.zzh.utils;

import com.zzh.es.Es5SinkFailureHandler;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-12 16:53
 **/
public class ElasticSearchSinkUtil {
    public static <T> void addSink(SingleOutputStreamOperator<T> data, int parallelism, Map<String, String> config, List<InetSocketAddress> hosts, ElasticsearchSinkFunction<T> function) {
        //ActionRequestFailureHandler failureHander = new RetryRejectedExecutionFailureHandler();
        ActionRequestFailureHandler failureHander = new Es5SinkFailureHandler();//失败处理策略
        ElasticsearchSink<T> sink = new ElasticsearchSink<>(config, hosts, function, failureHander);
        data.addSink(sink).setParallelism(parallelism);
    }

    public static List<InetSocketAddress> getEsAddresses(String hosts) throws Exception {
        String[] hostList = hosts.split(",");
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new InetSocketAddress(InetAddress.getByName(url.getHost()), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new InetSocketAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }
}
