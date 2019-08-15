package com.zzh.simple;

import com.zzh.domain.Tweet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-15 16:18
 **/
public class TweetCountPerLang {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "");
        props.setProperty(TwitterSource.TOKEN, "");
        props.setProperty(TwitterSource.TOKEN_SECRET, "");
        env.addSource(new TwitterSource(props))
                .map(new Map2Tweet())
                .keyBy(new KeySelector<Tweet, String>() {
                    @Override
                    public String getKey(Tweet tweet) throws Exception {
                        return tweet.getLang();
                    }
                })
                .timeWindow(Time.minutes(1))
                .apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tweet> input, Collector<Tuple3<String, Long, Date>> out) throws Exception {
                        long size = 0;
                        for (Tweet tweet : input) {
                            size++;
                        }
                        out.collect(new Tuple3<>(key, size, new Date(window.getEnd())));
                    }
                })
                .print();
    }

}
