import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of the user-based collaborative filtering system given in the slides
 */
public class UserBasedCollabFiltering {

    SQLiteConnection sql;
    HashMap<Integer, Float> currentUserRating;
    HashMap<Integer, Float> mapJ;
    public UserBasedCollabFiltering(SQLiteConnection sql) {
        this.sql = sql;
        sql.connect();
    }


    //The size of the batches before inserting into the DB
    public static final int BATCH_SIZE = 2;

    public void calculateSimilarRated(HashMap<Integer,HashMap<Integer,Float>> map) {
        //Keeps count of the amount of similarities which have been successfully calculated
        int amountCalculated = 0;

        sql.startTransaction();

        int start_i = 1;
        int start_j = start_i;

        // Last record entered:
        ArrayList<Integer> lastRecord = sql.getLastRecordFromSimilarityTable();

        //Get averages in memory:
        HashMap<Integer, Float> averagesMap = sql.getAveragesToMemory();

        if (!lastRecord.isEmpty()) {
            if (lastRecord.get(1) + 1 > map.entrySet().size()) { // If its the last entry for userA
                start_i++;
                start_j = start_i;

            } else {
                start_i = lastRecord.get(0);
                start_j = lastRecord.get(1);
            }

        }

        for (int i = start_i; i < map.entrySet().size() ; i++) {

            //This is the items which user i has rated
            currentUserRating = map.get(i);

            // Has the user rated some item(s)
            if (currentUserRating.size() > 0) {
                for (int j = start_j + 1; j <= map.entrySet().size(); j++) {

//                    System.out.println("i " + i);
//                    System.out.println("j " + j);

                    //These are the items which user j has rated
                    mapJ = map.get(j);

                    float topLine = 0;
                    float userACalc = 0;
                    float userBCalc = 0;
                    //There is a similarity between the 2 users (at least one item)
                    boolean similarityExists = false;
                    //Number of items which the users have both rated
                    int similarItemsRated = 0;


                    for (int key : mapJ.keySet()) {
                        if (currentUserRating.containsKey(key)) {

                            similarItemsRated++;
                            similarityExists = true;

                            //The calculation which concerns the item which the user i has rated negated by their average
                            float first = currentUserRating.get(key) - averagesMap.get(i);
//                          //The calculation which concerns the item which the user j has rated negated by their average
                            float second = mapJ.get(key) - averagesMap.get(j);

                            topLine += (first * second);
                            userACalc += Math.pow(first, 2);
                            userBCalc += Math.pow(second, 2);
                        }
                    }

                    if (similarityExists) {
                        float bottomLine = (float) (Math.sqrt(userACalc) * Math.sqrt(userBCalc));
                        try {
                            //If there is a NaN exception do not include it in the similarity matrix
                            if (bottomLine == 0) {
                                throw new NumberFormatException();

                            } else {
                                float similarity = (topLine / bottomLine);
                                amountCalculated++;
                                // Insert one value into the transaction block to insert
                                sql.insertSimilarityMeasure(i, j, similarity, similarItemsRated);

                            }

                        } catch (NumberFormatException e) {
                            System.err.println("user A: " + i + " user B: " + j + " has encountered a NaN exception");

                            //Regardless if there is a NaN exception this will execute and allow the transaction to end and it to be placed into the DB
                        } finally {
                            // Have 1000 items been calculated or has it reached the end of the table
                            if (amountCalculated % BATCH_SIZE == 0 || (i == map.entrySet().size() - 1 && j == map.entrySet().size())) {
                                sql.endTransaction();

                                // If it hasn't reached the end, start a new transaction
                                if (!(i == map.entrySet().size() - 1 && j == map.entrySet().size())) {
                                    sql.startTransaction();
                                }

                            }
                        }
                    }
                }

            }
            start_j = i + 1; // Reset the counter for a new person

        }


    }

//    //Works out the similarities between the users
//    public void similarityMeasure() {
//        sql.connect();
//
//        //Stores the average for both of the users
//        double userAAverage;
//        double userBAverage;
//        //The capacity being the size of the batches
//        HashMap<String, Double> similaritiesToAdd = new HashMap<String, Double>(BATCH_SIZE);
//
//        //Maximum number of iterations to create the similarity matrix
//        final int MAX_ITERATIONS = sql.getAmountOfRows("testSet1");
//
//        //Stores the items rated by 2 users.
//        HashMap<Integer, String> similarItemsRated;
//
//
//        for (int i = 1; i < MAX_ITERATIONS; i++) {
//            userAAverage = sql.getUserAverage(i);
//            for (int y = 1; y < MAX_ITERATIONS; y++) {
//                userBAverage = sql.getUserAverage(y);
//
//                //As to not produce duplicates in the similarity ratings table or to create similarities for the same users, i.e. i = 1, y = 1
//                if (i <= y) {
//                    //TODO This is selecting a value from the database each time, issue?
//                    //Finds the items which both users have rated and the rating associated with them for both users.
//                    similarItemsRated = sql.similarityValues(i, y);
//                    //Finds the items which both users have rated and the rating associated with them for both users. Returns a hashmap
////                    similarItemsRated = sql.similarityValues(i, y);
////                    HashMap<Integer, String> similarItemsRated = new HashMap<Integer, String>();
//
//
//                    double topLine = 0;
//                    double userACalc = 0;
//                    double userBCalc = 0;
//                    double similarity = 0;
//
//                    if (similarItemsRated.entrySet().size() > 1) {
//                        for (HashMap.Entry<Integer, String> entry : similarItemsRated.entrySet()) {
//                            //Ratings returned for both users for the same item
//                            double ratingA = Double.parseDouble(entry.getValue().split(",")[0]);
//                            double ratingB = Double.parseDouble(entry.getValue().split(",")[1]);
//
//                            double first = ratingA - userAAverage;
//                            double second = ratingB - userBAverage;
//
//                            topLine += (first * second);
//                            userACalc += Math.pow(first, 2);
//                            userBCalc += Math.pow(second, 2);
//                        }
//                        double bottomLine = Math.sqrt(userACalc) * Math.sqrt(userBCalc);
//
//                        similarity = (topLine / bottomLine);
//
//                        //Add to the similarities table only if we have not reached the end of the users
//                        if (i <= MAX_ITERATIONS && y <= MAX_ITERATIONS && !Double.isNaN(similarity)) {
//                            similaritiesToAdd.put(i + "," + y + "," + similarItemsRated.size(), similarity);
//                            similarItemsRated.clear();
//                        }
//
//                        if (similaritiesToAdd.size() % 1000 == 0 || (i == MAX_ITERATIONS & y == MAX_ITERATIONS)){
//                            System.out.println("Starting batch");
//                            for (HashMap.Entry<String, Double> entry : similaritiesToAdd.entrySet()) {
//                                int userA = Integer.parseInt(entry.getKey().split(",")[0]);
//                                int userB = Integer.parseInt(entry.getKey().split(",")[1]);
//                                int similarItems = Integer.parseInt(entry.getKey().split(",")[2]);
//                                double similarityRating = entry.getValue();
//
////                            System.out.println(userA + ", userB: " + userB + " y: " + y + ", similarity: " + similarity + ", similarItem: " + similarItems);
//
//                                sql.insertSimilarityValue(userA, userB, similarityRating, similarItems);
//                            }
//                            similaritiesToAdd.clear();
//                            System.out.println("DONE BATCH");
////                        if(i < MAX_ITERATIONS && y < MAX_ITERATIONS) {
////                            System.out.println("*********************DONE STACK*************************");
//////                            similarityMeasure(i, y);
////                        } else {
////                            System.out.println("FINISHED");
////                            return;
////                        }
//                        }
//
//                    }
////                    System.out.println("i= " + i + " y= " + y);
//                    //Batch processing (insertion)
//
//
//                }
//            }
//        }
//    }

    // Works out predicted rating for two users. map represents the predictedSet
    public void calculatePredictedRating(HashMap<Integer,HashMap<Integer,Float>> map) {
        long amountCalculated = 0;
        //The amount of users which have been processed regardless if they have/haven't got any shared item similarities (cold start problem)
        long rowsProcessed = 0;
        long numberOfRows = sql.getAmountOfRows(SQLiteConnection.PREDICTED_RATING_TABLE, "userID");
        // Training set which can be used to find the items which the neighbours have rated
        HashMap<Integer,HashMap<Integer,Float>> trainingSet = sql.getTrainingSetToMemory(SQLiteConnection.TRAINING_SET);
        sql.startTransaction();
        for (int user: map.keySet()) {
            HashMap<Integer, Float> itemMap = map.get(user);
            for (int item : itemMap.keySet()) {
                HashMap<Integer, Float> neighbourMap = sql.getNeighbourSelection(user);
                float meanA = sql.getUserAverage(user);
                float top = 0.0f;
                float bottom = 0.0f;
                boolean neighbourhoodItemValid = false;

                for(HashMap.Entry<Integer, Float> entry : neighbourMap.entrySet()) {
                    int userNew = entry.getKey();

                    // Has the neighbourhood user rated the item we are trying to predict a rating for
                    if (trainingSet.get(userNew).containsKey(item)) {

                        // From trainingSet, getting item rating for the item currently trying to predict from neighbour
                        float rating = trainingSet.get(userNew).get(item);

                        float similarity = entry.getValue();
                        top += similarity * (rating - sql.getUserAverage(userNew));
//                        top += similarity * (sql.getNeighbourhoodRated(userNew, user) - sql.getUserAverage(userNew));
                        bottom += similarity;
                        neighbourhoodItemValid = true;

                    }
                }
                rowsProcessed++;

                if (neighbourhoodItemValid) {
                    float rating = meanA + (top/bottom);
                    sql.insertPredictedRating(user, item, rating);
                    //Amount currently in the batch
                    amountCalculated++;
                } else {
                    // If the user doesn't have any suitable neighbours then we simply insert the average value given by that user
                    sql.insertPredictedRating(user, item, sql.getUserAverage(user));
                }
                if (amountCalculated % BATCH_SIZE == 0 || (rowsProcessed == numberOfRows)) {
                    sql.endTransaction();

                    if (rowsProcessed != numberOfRows) {
                        sql.startTransaction();
                    }
                }
            }
        }

//        //TODO Needs to check if user has actually rated item, if so don't calculate
//        HashMap<Integer, Float> neighbourMap = sql.getNeighbourSelection(user);
//        float meanA = sql.getUserAverage(user);
//        float top = 0.0f;
//        float bottom = 0.0f;
//        for(HashMap.Entry<Integer, Float> entry : neighbourMap.entrySet()) {
//            int userNew = entry.getKey();
//            float similarity = entry.getValue();
//            top += similarity * (sql.getNeighbourhoodRated(userNew, item) - sql.getUserAverage(userNew));
//            bottom += similarity;
//        }
//        float rating = meanA + (top/bottom);
////        System.out.println(meanA + (top/bottom));
//        sql.insertPredictedRating(user, item, rating);
//        sql.closeConnection();

    }

    public void computeAllAverages(HashMap<Integer,HashMap<Integer,Float>> map) {
        sql.startTransaction();
        for (int i = 1; i <= map.entrySet().size() ; i++) {
            sql.computeAverageForUser(i);
            if (i % BATCH_SIZE == 0 || (i == map.entrySet().size())) {
                sql.endTransaction();

                if (!(i == map.entrySet().size())) {
                    sql.startTransaction();
                }
            }

        }

    }

}
