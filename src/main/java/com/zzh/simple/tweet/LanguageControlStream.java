package com.zzh.simple.tweet;

import com.zzh.domain.TweetWithTags;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-16 13:54
 **/
public class LanguageControlStream {
    public static void main(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "...");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
        props.setProperty(TwitterSource.TOKEN, "...");
        props.setProperty(TwitterSource.TOKEN_SECRET, "...");

        DataStream<LanguageConfig> controlStream = env.socketTextStream("localhost", 9876)
                .flatMap(new FlatMapFunction<String, LanguageConfig>() {
                    @Override
                    public void flatMap(String value, Collector<LanguageConfig> out) throws Exception {

                        for (String languageConfig : value.split(",")) {
                            String[] kvPair = languageConfig.split("=");
                            out.collect(new LanguageConfig(kvPair[0], Boolean.parseBoolean(kvPair[1])));
                        }
                    }
                });


        env.addSource(new TwitterSource(props))
                .map(new Map2TweetWithTags())
                .keyBy(new KeySelector<TweetWithTags, Object>() {
                    @Override
                    public Object getKey(TweetWithTags tweet) throws Exception {
                        return tweet.getLang();
                    }
                })
                .connect(controlStream.keyBy(new KeySelector<LanguageConfig, Object>() {
                    @Override
                    public Object getKey(LanguageConfig langConfig) throws Exception {
                        return langConfig.getLanguage();
                    }
                }))
                .flatMap(new RichCoFlatMapFunction<TweetWithTags, LanguageConfig, Tuple2<String, String>>() {
                    ValueStateDescriptor<Boolean> shouldProcess = new ValueStateDescriptor<Boolean>("languageConfig", Boolean.class);

                    @Override
                    public void flatMap1(TweetWithTags tweet, Collector<Tuple2<String, String>> out) throws Exception {
                        Boolean processLanguage = getRuntimeContext().getState(shouldProcess).value();
                        if (processLanguage != null && processLanguage) {
                            for (String tag : tweet.getTags()) {
                                out.collect(new Tuple2<>(tweet.getLang(), tag));
                            }
                        }
                    }

                    @Override
                    public void flatMap2(LanguageConfig config, Collector<Tuple2<String, String>> out) throws Exception {
                        getRuntimeContext().getState(shouldProcess).update(config.isShouldProcess());
                    }
                })
                .print();

        env.execute();
    }

    static class LanguageConfig {
        private String language;
        private boolean shouldProcess;

        public LanguageConfig(String language, boolean shouldProcess) {
            this.language = language;
            this.shouldProcess = shouldProcess;
        }

        public String getLanguage() {
            return language;
        }

        public boolean isShouldProcess() {
            return shouldProcess;
        }
    }
}
