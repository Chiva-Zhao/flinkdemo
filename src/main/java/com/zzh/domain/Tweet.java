package com.zzh.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-15 15:31
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Tweet {
    private String text;
    private String lang;
}