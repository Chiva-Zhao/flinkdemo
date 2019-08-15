package com.zzh.simple;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-15 9:13
 **/
public class Top10MoviesPartition {
    private static final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    private static final String folder = "ml-latest-small/";

    public static void main(String[] args) throws Exception {
        MapPartitionOperator<Tuple2<Long, Double>, Tuple2<Long, Double>> sorted = environment.readCsvFile(folder + "ratings.csv").ignoreFirstLine()
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
                })
                .partitionCustom(new Partitioner<Double>() {
                    @Override
                    public int partition(Double key, int numPartition) {
                        return key.intValue() - 1;
                    }
                }, 1)
                .setParallelism(5)
                .sortPartition(1, Order.DESCENDING)
                .mapPartition(new MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Long, Double>> iterable, Collector<Tuple2<Long, Double>> collector) throws Exception {
                        Iterator<Tuple2<Long, Double>> iterator = iterable.iterator();
                        for (int i = 0; i < 10 && iterator.hasNext(); i++) {
                            collector.collect(iterator.next());
                        }
                    }
                })
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .mapPartition(new MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Long, Double>> iterable, Collector<Tuple2<Long, Double>> collector) throws Exception {
                        Iterator<Tuple2<Long, Double>> iterator = iterable.iterator();
                        for (int i = 0; i < 10 && iterator.hasNext(); i++) {
                            collector.collect(iterator.next());
                        }
                    }
                });

        DataSource<Tuple2<Long, String>> movies = environment.readCsvFile(folder + "movies.csv").ignoreFirstLine().ignoreInvalidLines()
                .parseQuotedStrings('"').includeFields(true, true, false).types(Long.class, String.class
                );

        movies.join(sorted).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, Double>, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> join(Tuple2<Long, String> movie, Tuple2<Long, Double> sort) throws Exception {
                        return new Tuple3<>(movie.f0, movie.f1, sort.f1);
                    }
                }).print();
        //sorted.join(movies).where(0).equalTo(0)
        //        .with(new JoinFunction<Tuple2<Long, Double>, Tuple2<Long, String>, Tuple3<Long, String, Double>>() {
        //            @Override
        //            public Tuple3<Long, String, Double> join(Tuple2<Long, Double> sort, Tuple2<Long, String> movie) throws Exception {
        //                return new Tuple3<>(movie.f0, movie.f1, sort.f1);
        //            }
        //        }).print();
    }
}
