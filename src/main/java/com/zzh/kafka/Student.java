package com.zzh.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-6 20:23
 **/
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Student {
    private int id;
    private String name;
    private String password;
    private int age;
}
