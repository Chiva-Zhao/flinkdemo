package com.zzh.simple.tweet;

import com.zzh.domain.Tweet;
import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

class Map2Tweet implements MapFunction<String, Tweet> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Tweet map(String jsonTweet) throws Exception {
        JsonNode node = mapper.readTree(jsonTweet);
        JsonNode textNode = node.get("text");
        JsonNode langNode = node.get("land");
        return new Tweet(textNode.asText(), langNode.asText());
    }
}