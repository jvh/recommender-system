import java.sql.*;
import java.util.ArrayList;
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

    public void insertSimilarityValue(int userA, int userB, double similarityValue) {
        String insert = "INSERT INTO similaritySet VALUES (" + userA + "," + userB + "," + similarityValue + ")";
        try {
            Statement insertStatement = connection.createStatement();
            insertStatement.executeUpdate(insert);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public double getUserAverage(int userID) {
        double average = 0;
        int resultCount = 0;
        try {
            String query = "SELECT averageValue FROM averageSet WHERE userID=" + userID + " AND userID <> 0";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            boolean empty = true;
            while(resultSet.next() ) {
                average += resultSet.getDouble(1);
                resultCount++;
                empty = false;
            }
//
            if(empty) {
                String queryOther = "SELECT realRating FROM trainingSet WHERE userID=" + userID + " AND userID <> 0";
                Statement statementOther = connection.createStatement();
                ResultSet resultSetOther = statementOther.executeQuery(queryOther);
                while(resultSetOther.next()) {
                    average += resultSetOther.getDouble(1);
                    resultCount++;
                }
                average = average / resultCount;
                String insert = "INSERT INTO averageSet VALUES (" + userID + "," + average + ")";
                Statement insertStatement = connection.createStatement();
                insertStatement.executeUpdate(insert);

            }
            System.out.println(average);
            return average;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return average;
    }



    public HashMap<Integer, String> similarityValues(int userA, int userB) {
        HashMap<Integer, String> resultMap = new HashMap<>();

        try {

            String query = "SELECT t1.* FROM trainingSet AS t1 JOIN (SELECT itemID, realRating FROM trainingSet WHERE userID in (" + userA + "," + userB + ") GROUP BY itemID HAVING ( COUNT(itemID) > 1)) AS t2 ON t1.itemID = t2.itemID WHERE userID IN (" + userA + "," + userB + ") ORDER BY itemID";

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while(resultSet.next()) {
                int itemID = resultSet.getInt(2);
                if(!resultMap.containsKey(itemID)) {
                    double valueA = resultSet.getInt(3);
                    resultSet.next();
                    double valueB = resultSet.getInt(3);
                    String result = valueA + "," + valueB;
                    resultMap.put(itemID, result);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(resultMap);
        return resultMap;
    }

    public HashMap<Integer, HashMap<Integer,Integer>> getAmountOfRecords(String tableName, int amount) {
        HashMap<Integer, HashMap<Integer, Integer>> userIdMap = new HashMap<>();
        try {

            int userid1 = 232;
            int userid2 = 543;
//            String query2 = "SELECT * FROM" + " " + tableName + " WHERE itemID IN (434,879);


            String query = "SELECT * FROM" + " " + tableName + " LIMIT " + amount;

            Statement statement = connection.createStatement();
//            statement.setQueryTimeout(30);

//            ResultSet resultSet = preparedStatement.executeQuery();
//            System.out.println(resultSet);
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
//        sqLiteConnection.getUserAverage(2);
//        sqLiteConnection.getUserAverage("averageSet", 1);
//        sqLiteConnection.similarityValues(49, 124);
        sqLiteConnection.insertSimilarityValue(1,2, 343.432);
//        sqLiteConnection.intersectTest();

//        sqLiteConnection.getAmountOfRecords("trainingSet", 100);
//        System.out.println(sqLiteConnection.getAmountOfRecords("trainingSet", 100));
//        System.out.println(sqLiteConnection.getUserAverage("trainingSet", 1));
//        )sqLiteConnection.getUserId("trainingSet", 1);
        sqLiteConnection.closeConnection();
//        connect();
//        getAmountOfRecords("minimalTestSet", 15);
//        closeConnection();
    }

}
