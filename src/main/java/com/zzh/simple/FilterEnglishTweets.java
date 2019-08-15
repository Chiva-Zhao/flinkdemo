package com.zzh.simple;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zzh.domain.Tweet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-15 14:54
 **/
public class FilterEnglishTweets {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "");
        props.setProperty(TwitterSource.TOKEN, "");
        props.setProperty(TwitterSource.TOKEN_SECRET, "");
        env.addSource(new TwitterSource(props))
                .map(new Map2Tweet())
                .filter(new FilterFunction<Tweet>() {
                    @Override
                    public boolean filter(Tweet tweet) throws Exception {
                        return tweet.getLang().equals("en");
                    }
                }).print();
    }

    private static class Map2Tweet implements MapFunction<String, Tweet> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Tweet map(String jsonTweet) throws Exception {
            JsonNode node = mapper.readTree(jsonTweet);
            JsonNode textNode = node.get("text");
            JsonNode langNode = node.get("land");
            return new Tweet(textNode.asText(), langNode.asText());
        }
    }
}


