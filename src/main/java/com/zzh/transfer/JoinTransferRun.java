package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 17:26
 **/
public class JoinTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).setParallelism(1)
                .join(env.addSource(new SourceFromMySQL()))
                .where(Student::getId)
                .equalTo(st -> st.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((st1, st2) -> {
                    return st1.getAge() + st2.getAge() / 2;
                })
                .print();

        env.execute("windowRun");
    }
}