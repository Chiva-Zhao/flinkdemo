package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-8 15:15
 **/
public class FoldTransfer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.addSource(new SourceFromMySQL()).setParallelism(1)
                .keyBy(Student::getAge).fold(30, (val, st) -> {
            val += st.getAge();
            return val;
        }).print();
        environment.execute("foldRun");
    }
}
