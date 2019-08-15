package com.zzh.simple.tweet;

import com.zzh.domain.TweetWithTags;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-15 17:39
 **/
public class TopHashTagTweet {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "");
        props.setProperty(TwitterSource.TOKEN, "");
        props.setProperty(TwitterSource.TOKEN_SECRET, "");
        env.addSource(new TwitterSource(props))
                .map(new Map2TweetWithTags())
                .flatMap(new FlatMapFunction<TweetWithTags, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(TweetWithTags tweetWithTags, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        tweetWithTags.getTags().forEach(tag -> collector.collect(new Tuple2<>(tag, 1)));
                    }
                })
                //.print();
                .keyBy(0)
                .timeWindow(Time.minutes(10))
                .sum(1)
                //.print();
                .timeWindowAll(Time.minutes(10))
                .apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple3<Date, String, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<Date, String, Integer>> out) throws Exception {
                        String topTag = null;
                        int count = 0;
                        for (Tuple2<String, Integer> item : iterable) {
                            if (item.f1 > count) {
                                count = item.f1;
                                topTag = item.f0;
                            }
                        }
                        out.collect(new Tuple3<>(new Date(window.getEnd()), topTag, count));
                    }
                }).print();
    }
}
