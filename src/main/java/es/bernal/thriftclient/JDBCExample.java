package es.bernal.thriftclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;

/**
 * Created by bernal on 15/8/17.
 */
public class JDBCExample {

    public static Connection connection;

    public static void main(String[] argv) {

        System.out.println("-------- Spark-Hive Thrift Server JDBC Connection Testing ------------");

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Spark-Hive Thrift Server JDBC Driver?");
            e.printStackTrace();
            return;
        }

        System.out.println("Spark-Hive Thrift Server JDBC Driver Registered!");
        connection = null;

        String ipAddress = "192.168.1.126";
        int port = 10002;
        if (argv.length == 2) {
            ipAddress = argv[0];
            port = Integer.parseInt(argv[1]);
        } else {
            System.out.println("default connection params setted!!");
        }

        try {
            connection = DriverManager.getConnection("jdbc:hive2://" + ipAddress + ":" + port);

        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        if (connection != null) {
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection!");
        }

        BufferedReader br = null;

        try {

            br = new BufferedReader(new InputStreamReader(System.in));

            boolean exit =false;
            while (!exit) {

                System.out.print("<sql> ");
                String input = br.readLine();

                if ("q".equals(input)) {
                    System.out.println("Exit!");
                    exit = true;
                } else {
                    System.out.println("input command: " + input);
                    executeQuery(input);
                    System.out.println("-----------\n");
                }
            }

        } catch (IOException e) {
            System.out.println("IO Console Error!pe");
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            connection.close();
            System.out.println("Connection succesfuly closed");
        } catch (SQLException e) {
            System.out.println("Error while closing connection");
        }
    }

    private static void executeQuery(String selectSQL) {
        // Execute a query;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
            ResultSet rs = preparedStatement.executeQuery(selectSQL);
            int count = 0;


            ResultSetMetaData rsmd = rs.getMetaData();
            int numColumns = rsmd.getColumnCount();
            System.out.print("| ");
            for (int i = 1; i<=numColumns; i++) {
                System.out.print(rsmd.getColumnName(i) + " | ");
            }
            System.out.println("");
            System.out.println("|------------------------------------------------");
            while (rs.next()) {
                System.out.print("| ");
                for (int i = 1; i<=numColumns; i++) {
                    System.out.print(rs.getString(i) + " | ");
                }
                System.out.println("");
                count++;
            }
            System.out.println(count + " registers retrieved");
        } catch (SQLException e) {
            System.out.println("Error in Query!");
        }
    }

}
