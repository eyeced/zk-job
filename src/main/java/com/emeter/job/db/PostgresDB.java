package com.emeter.job.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The Class
 * User: abhinav
 * Date: 3/30/14
 * Time: 10:33 PM
 */
public class PostgresDB {

    private Connection connection;

    private String url;

    private String userName;

    private String password;

    public PostgresDB(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        try {
            createConnection();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void createConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(url, userName, password);
    }

    public void closeConnection() throws SQLException {
        connection.close();
    }

    /**
     * get the connection
     * @return
     */
    public Connection getConnection() {
        return this.connection;
    }
}
