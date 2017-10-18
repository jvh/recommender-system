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

    public HashMap<Integer, HashMap<Integer,Integer>> getAmountOfRecords(String tableName, int amount) {
        HashMap<Integer, HashMap<Integer, Integer>> userIdMap = new HashMap<>();
        String query = "SELECT * FROM" + " " + tableName + " LIMIT " + amount;
        try {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);

            ResultSet resultSet = statement.executeQuery(query);
            int currentUserId = 0;

            while(resultSet.next()) {

                HashMap<Integer, Integer> pairRatingsMap;
                int foundUserId = resultSet.getInt(1);

                if(foundUserId != currentUserId) {
                    currentUserId = foundUserId;
                    pairRatingsMap = new HashMap<>();
                    userIdMap.put(foundUserId, pairRatingsMap);

                } else {
                    pairRatingsMap = userIdMap.get(foundUserId);
                }
                pairRatingsMap.put(resultSet.getInt(2), resultSet.getInt(3));

            }

//            System.out.println(userIdMap.entrySet().size());
//            for(int i = 1 ; i <= userIdMap.entrySet().size(); i++) {
//                System.out.println(userIdMap.get(i));
//            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return userIdMap;
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
        sqLiteConnection.getAmountOfRecords("trainingSet", 100);
        sqLiteConnection.closeConnection();
//        connect();
//        getAmountOfRecords("minimalTestSet", 15);
//        closeConnection();
    }
}
