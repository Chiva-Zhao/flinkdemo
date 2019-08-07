package com.zzh.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;
/**
 * @author zhaozh
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Metric {
    public String name;
    public long timestamp;
    public Map<String, Object> fields;
    public Map<String, String> tags;
}