package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 19:33
 **/
public class SplitTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SplitStream<Student> allStream = environment.addSource(new SourceFromMySQL()).setParallelism(1)
                //.split(st -> Arrays.asList(st.getName().split("\\,")))
                .split(st -> {
                    List<String> out = new ArrayList<>();
                    if (st.getId() % 2 == 0) {
                        out.add("even");
                    } else {
                        out.add("odd");
                    }
                    return out;
                });
        DataStream<Student> oddStream = allStream.select("odd");
        DataStream<Student> evenStream = allStream.select("even");
        DataStream<Student> all = allStream.select("even","odd");
        oddStream.print();
        evenStream.print();
        environment.execute("splitRun");
    }
}
