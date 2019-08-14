package com.zzh.simple;

import com.zzh.domain.Movie;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-14 14:29
 **/
public class FilterMovies {
    private static final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    private static final String folder = "ml-latest-small/";

    public static void main(String[] args) throws Exception {
        DataSource<Tuple3<Long, String, String>> dataSourceMovies = environment.readCsvFile(folder + "movies.csv")
                .ignoreFirstLine().ignoreInvalidLines()
                .parseQuotedStrings('"')
                .types(Long.class, String.class, String.class);
        dataSourceMovies.map(FilterMovies::makeMovie)
                .filter(movie -> movie.getGenres().contains("Drama"))
                .writeAsText(folder + "drama.csv", FileSystem.WriteMode.OVERWRITE);
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
