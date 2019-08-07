package com.zzh.mysql;

import com.zzh.domain.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zhaozh
 * @version 1.0
 * @date 2019-8-7 17:21
 **/
public class SinkToMysql extends RichSinkFunction<Student> {
    private PreparedStatement ps;
    private Connection connection;
    private String insertSql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        ps = connection.prepareStatement(insertSql);
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

    @Override
    public void invoke(Student student, Context context) throws Exception {
        ps.setInt(1, student.getId());
        ps.setString(2, student.getName());
        ps.setString(3, student.getPassword());
        ps.setInt(4, student.getAge());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/jusfoun_dome_febs?serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8","root","cz186008");
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
