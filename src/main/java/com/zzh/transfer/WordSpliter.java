package com.zzh.transfer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-18 11:06
 **/
public class WordSpliter implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String sentence, Collector<String> collector) throws Exception {
        for (String word : sentence.split(" ")) {
            collector.collect(word);
        }
    }
}
