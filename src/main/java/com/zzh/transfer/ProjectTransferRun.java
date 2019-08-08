package com.zzh.transfer;

import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 20:06
 **/
public class ProjectTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).setParallelism(1)
                .project(3, 2).print();
        env.execute("projectRun");
    }
}
