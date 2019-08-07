package com.zzh.mysql;

import com.zzh.domain.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-6 20:24
 **/
public class SourceFromMySQL extends RichSourceFunction<Student> {

    private Connection connection;
    private PreparedStatement ps;
    private boolean running = true;

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet rs = ps.executeQuery();
        while (running) {
            while (rs.next()) {
                Student student = new Student(rs.getInt("id"), rs.getString("name"),
                        rs.getString("password"), rs.getInt("age"));
                ctx.collect(student);
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from Student;";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/jusfoun_dome_febs?serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8", "root", "cz186008");
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
