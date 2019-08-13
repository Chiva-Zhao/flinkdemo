package com.zzh.watermarks;

import com.zzh.domain.MetricEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-13 10:50
 **/
public class MetricWatermark implements AssignerWithPeriodicWatermarks<MetricEvent> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }

    @Override
    public long extractTimestamp(MetricEvent element, long previousElementTimestamp) {
        if (element.getTimestamp() > currentTimestamp) {
            currentTimestamp = element.timestamp;
        }
        return currentTimestamp;
    }
}
