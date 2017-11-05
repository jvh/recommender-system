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

    public int getAmountOfRows(String tableName) {
        int count = 0;
        String query = "SELECT COUNT(userID) FROM " + tableName;
        try {
            Statement queryStatement = connection.createStatement();
            ResultSet resultSet = queryStatement.executeQuery(query);
            while(resultSet.next()) {
                count = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }
    //TODO Batch process to 1000 items per insertion
    public void insertSimilarityValue(int userA, int userB, double similarityValue, int amountOfSimilarItemsRated) {
        String insert = "INSERT INTO similaritySet VALUES (" + userA + "," + userB + "," + similarityValue + "," + amountOfSimilarItemsRated + ")";
        try {
            Statement insertStatement = connection.createStatement();
            insertStatement.executeUpdate(insert);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public HashMap<Integer, Double> getNeighbourSelection(int userID) {
        HashMap<Integer, Double> resultMap = new HashMap<>();
//        String query = "SELECT userB, similarityValue, similarItemsAmount FROM similaritySet WHERE userA = " + userID + " AND similarityValue > 0 AND similarItemsAmount >= 2 ORDER BY similarityValue DESC LIMIT 20";
//        String query = "SELECT userB, similarityValue, similarItemsAmount FROM similaritySet WHERE userA = " + userID + " AND similarityValue > 0 AND similarItemsAmount >= 2 ORDER BY similarItemsAmount DESC, similarityValue DESC LIMIT 20";
        String query = "SELECT userB, similarityValue, similarItemsAmount FROM similaritySet WHERE userA = " + userID + " AND similarityValue > 0 AND similarItemsAmount >= 2 ORDER BY (.05 * similarItemsAmount) + (.95 * similarityValue) DESC LIMIT 20";
        try {
            Statement queryStatement = connection.createStatement();
            ResultSet resultSet = queryStatement.executeQuery(query);
            while(resultSet.next()) {
                int userB = resultSet.getInt(1);
                double similarityValue = resultSet.getDouble(2);
                resultMap.put(userB, similarityValue);
//                System.out.println(resultSet.getInt(1) + " " + resultSet.getDouble(2) + " " + resultSet.getInt(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    public void insertPredictedRating(int userID, int itemID, double rating, String tableName) {
        String update = "UPDATE " +  tableName + "  SET predictedRating = " + rating + " WHERE userID = " + userID + " AND itemID = " + itemID;
        try {
            Statement updateStatement = connection.createStatement();
            updateStatement.executeUpdate(update);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Double getNeighbourhoodRated(int userID, int itemID) {
        String query = "SELECT realRating FROM trainingSET WHERE userID = " + userID + " AND itemID = " + itemID;
        double result = 0;
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            while(resultSet.next()) {
                result = resultSet.getDouble(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
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
//            System.out.println(average);
            return average;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return average;
    }

    public void createTestDatabase(String tableName, int size) {
        String create = "CREATE TABLE " + tableName + " (userID INTEGER, itemID INTEGER, realRating REAL, predictedRating REAL)";
        String insert = "INSERT INTO " + tableName + " SELECT * FROM trainingSet LIMIT " + size;
        try {
            Statement createStatement = connection.createStatement();
            createStatement.executeUpdate(create);
            Statement insertStatement = connection.createStatement();
            insertStatement.executeUpdate(insert);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteTestDatabase(String tableName) {
        String delete = "DROP TABLE " + tableName;
        try {
            Statement deleteStatement = connection.createStatement();
            deleteStatement.executeUpdate(delete);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public HashMap<Integer, String> similarityValues(int userA, int userB) {
        HashMap<Integer, String> resultMap = new HashMap<>();

        try {

            String query = "SELECT t1.* FROM trainingSet AS t1 JOIN (SELECT itemID, realRating FROM trainingSet WHERE userID in (" + userA + "," + userB + ") GROUP BY itemID HAVING ( COUNT(itemID) > 1)) AS t2 ON t1.itemID = t2.itemID WHERE userID IN (" + userA + "," + userB + ") ORDER BY itemID ASC";
//             String query = "SELECT t1.* FROM testSet1 AS t1 JOIN (SELECT itemID, realRating FROM testSet1 WHERE userID in (" + userA + "," + userB + ") GROUP BY itemID HAVING ( COUNT(itemID) > 1)) AS t2 ON t1.itemID = t2.itemID WHERE userID IN (" + userA + "," + userB + ") ORDER BY itemID ASC";
// String query = "SELECT t1.* FROM trainingSet AS t1 JOIN (SELECT itemID, realRating FROM trainingSet GROUP BY itemID HAVING ( COUNT(itemID) > 1)) AS t2 ON t1.itemID = t2.itemID ORDER BY userID";
//            String query = "SELECT t1.* FROM trainingSet AS t1 JOIN (SELECT itemID, realRating FROM trainingSet WHERE userID BETWEEN 1 AND 1000 GROUP BY itemID HAVING ( COUNT(itemID) > 1)) AS t2 ON t1.itemID = t2.itemID WHERE userID BETWEEN 1 AND 1000 ORDER BY userID";

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while(resultSet.next()) {
//                System.out.println(resultSet.getInt(1));
//                System.out.println(resultSet.getInt(1) + " " + resultSet.getInt(2) + " " + resultSet.getInt(3));
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
        return resultMap;
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
//        sqLiteConnection.getNeighbourhoodRated(1,62440);
//        sqLiteConnection.getAmountOfRows("trainingSet");
//        System.out.println(sqLiteConnection.similarityValues(49, 97));
//        sqLiteConnection.insertPredictedRating(1, 62440, 6.2, "testSet1");
//        sqLiteConnection.createTestDatabase("testSet1", 10000);
//        sqLiteConnection.getAmountOfRows("testSet1");
//        sqLiteConnection.getUserAverage(2);
//        sqLiteConnection.getUserAverage("averageSet", 1);
//        sqLiteConnection.getNeighbourSelection(1);
//        System.out.println(sqLiteConnection.similarityValues(1, 16));
//        sqLiteConnection.insertSimilarityValue(1,2, 343.432);
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
