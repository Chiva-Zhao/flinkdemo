package com.zzh.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-19 15:00
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class WordCount {
    private String word;
    private Integer count;
}
