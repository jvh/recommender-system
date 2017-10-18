import java.sql.*;
import java.util.HashMap;

public class SQLiteConnection {

    public Connection connection = null;

    public void connect() {

        try {
            String url = "jdbc:sqlite:datasets.db";
            connection = DriverManager.getConnection(url);
            System.out.println("Connection established");

        } catch (SQLException e) {
            e.printStackTrace();

        }
    }

    public void getAmountOfRecords(String tableName, int amount) {
        HashMap<Integer, HashMap<Integer, Integer>> userIdMap = new HashMap<>();
        HashMap<Integer, Integer> pairRatingsMap = new HashMap<>();
        String query = "SELECT * FROM" + " " + tableName + " LIMIT " + amount;
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);

            ResultSet resultSet = statement.executeQuery(query);
            while(resultSet.next()) {
                System.out.println(resultSet.getInt(1));
                System.out.println(resultSet.getInt(2));
                System.out.println(resultSet.getInt(3));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SQLiteConnection sqLiteConnection = new SQLiteConnection();
        sqLiteConnection.connect();
        sqLiteConnection.getAmountOfRecords("minimalTestSet", 15);
        sqLiteConnection.closeConnection();
//        connect();
//        getAmountOfRecords("minimalTestSet", 15);
//        closeConnection();
    }
}
