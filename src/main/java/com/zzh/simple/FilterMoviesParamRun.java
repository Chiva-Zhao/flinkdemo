package com.zzh.simple;

import com.zzh.domain.Movie;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-15 13:48
 **/
public class FilterMoviesParamRun {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String input = parameterTool.get("input");
        String output = parameterTool.get("output");
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.readCsvFile(input)
                .ignoreFirstLine().ignoreInvalidLines()
                .parseQuotedStrings('"')
                .types(Long.class, String.class, String.class)
                .map(FilterMoviesParamRun::makeMovie)
                .filter(movie -> movie.getGenres().contains("Drama"))
                .writeAsText(output);
        environment.execute("fromElementRun");
    }

    private static Movie makeMovie(Tuple3<Long, String, String> line) {
        Movie movie = new Movie();
        movie.setMovieId(line.f0);
        movie.setTitle(line.f1);
        movie.setGenres(new HashSet<>(Arrays.asList(line.f2.split("\\|"))));
        return movie;
    }
}
