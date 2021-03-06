package com.zzh.simple.movie;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-14 17:55
 * find what movie genre receives better reviews
 **/
public class OptimizedAggRaitingMovieRun {
    private static final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    private static final String folder = "ml-latest-small/";

    public static void main(String[] args) throws Exception {
        DataSource<Tuple3<Long, String, String>> movies = environment.readCsvFile(folder + "movies.csv")
                .ignoreFirstLine().ignoreInvalidLines()
                .parseQuotedStrings('"')
                .types(Long.class, String.class, String.class);
        DataSource<Tuple2<Long, Double>> ratings = environment.readCsvFile(folder + "ratings.csv")
                .ignoreInvalidLines()
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class);
        //join result: movieName,movie genra and movie rating
        List<Tuple2<String, Double>> distribution = movies.join(ratings).where(0).equalTo(0).with(new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<StringValue, StringValue, DoubleValue>>() {
            @Override
            public Tuple3<StringValue, StringValue, DoubleValue> join(Tuple3<Long, String, String> movieTuple, Tuple2<Long, Double> ratingTuple) throws Exception {
                StringValue movieName = new StringValue(movieTuple.f1);
                StringValue genra = new StringValue(movieTuple.f2.split("\\|")[0]);
                DoubleValue rating = new DoubleValue(ratingTuple.f1);
                return new Tuple3<>(movieName, genra, rating);
            }
        })
                //.print();
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple3<StringValue, StringValue, DoubleValue>, Tuple2<String, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<StringValue, StringValue, DoubleValue>> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
                        String genra = null;
                        int count = 0;
                        double score = 0d;
                        for (Tuple3<StringValue, StringValue, DoubleValue> item : iterable) {
                            genra = item.f1.getValue();
                            score += item.f2.getValue();
                            count++;
                        }
                        collector.collect(new Tuple2<>(genra, score / count));
                    }
                }).collect();
        String rst = distribution.stream()
                .sorted((r1, r2) -> Double.compare(r1.f1, r2.f1))
                .map(Objects::toString)
                .collect(Collectors.joining("\n"));
        System.out.println(rst);
    }
}
