package com.zzh.transfer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-18 11:13
 **/
public class WordCountSpliter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
        for (String word : sentence.split("\\W+")) {
            collector.collect(new Tuple2<>(word, 1));
        }
    }
}
