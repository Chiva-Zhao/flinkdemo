package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 15:04
 **/
public class ReduceTransferRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.addSource(new SourceFromMySQL()).setParallelism(1)
                .keyBy(Student::getAge)
                .reduce((st1, st2) -> {
                    st1.setId(st1.getId()+st2.getId());
                    st1.setAge((st1.getAge() + st2.getAge()) / 2);
                    return st1;
                }).print();

        environment.execute("reduceRun");
    }
}
