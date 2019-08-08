package com.zzh.transfer;

import com.zzh.domain.Student;
import com.zzh.mysql.SourceFromMySQL;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class FilterTransfer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Student> student = environment.addSource(new SourceFromMySQL())
                .setParallelism(1)
                .filter(st -> st.getId() > 50);
        student.addSink(new PrintSinkFunction());
        environment.execute("filterRun");
    }
}