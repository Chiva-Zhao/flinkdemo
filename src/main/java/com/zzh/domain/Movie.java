package com.zzh.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Set;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-14 16:31
 **/
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Movie {
    private Long movieId;
    private String title;
    private Set<String> genres;
}
