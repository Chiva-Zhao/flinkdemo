package com.zzh.simple.movie;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-14 20:30
 **/
public class Top10Movies {
    private static final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    private static final String folder = "ml-latest-small/";

    public static void main(String[] args) throws Exception {
        List<Tuple2<Long, Double>> tupleResult = environment.readCsvFile(folder + "ratings.csv").ignoreFirstLine()
                .ignoreInvalidLines()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class)
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Double>> rows, Collector<Tuple2<Long, Double>> collector) throws Exception {
                        Long movieId = null;
                        int count = 0;
                        double score = 0;
                        for (Tuple2<Long, Double> item : rows) {
                            movieId = item.f0;
                            score += item.f1;
                            count++;
                        }
                        if (count > 50) {
                            collector.collect(new Tuple2<>(movieId, score / count));
                        }
                    }
                }).collect();
        tupleResult.stream().sorted(Comparator.comparingDouble(r -> -r.f1))
                .limit(10)
                .forEach(System.out::println);
    }
}
