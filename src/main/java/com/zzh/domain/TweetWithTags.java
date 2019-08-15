package com.zzh.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-15 16:59
 **/
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TweetWithTags {
    private String text;
    private String lang;
    private List<String> tags;
}
