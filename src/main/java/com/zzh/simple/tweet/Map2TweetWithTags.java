package com.zzh.simple.tweet;

import com.zzh.domain.TweetWithTags;
import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class Map2TweetWithTags implements MapFunction<String, TweetWithTags> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public TweetWithTags map(String jsonTweet) throws Exception {
        JsonNode node = mapper.readTree(jsonTweet);
        JsonNode textNode = node.get("text");
        JsonNode langNode = node.get("land");
        JsonNode entities = node.get("entities");
        List<String> tags = new ArrayList<>();
        if (entities != null) {
            JsonNode hastags = entities.get("hasTags");
            for (Iterator<JsonNode> iterator = hastags.getElements(); iterator.hasNext(); ) {
                String tag = iterator.next().get("text").getTextValue();
                tags.add(tag);
            }
        }
        return new TweetWithTags(textNode.asText(), langNode.asText(), tags);
    }
}