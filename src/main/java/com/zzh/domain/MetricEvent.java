package com.zzh.domain;

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
public class MetricEvent {
    public String name;
    public long timestamp;
    public Map<String, Object> fields;
    public Map<String, String> tags;
}