package com.zzh.transfer;

import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 17:10
 * union tow stream
 **/
public class UnionTransRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).setParallelism(1)
                .union(env.addSource(new SourceFromMySQL()));
        env.execute("unionRun");
    }
}
