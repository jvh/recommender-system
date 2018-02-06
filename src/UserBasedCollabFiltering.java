import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of the user-based collaborative filtering system given in the slides
 */
public class UserBasedCollabFiltering extends CollabFiltering {

    SQLiteConnection sql;
    HashMap<Integer, Float> currentUserRating;
    HashMap<Integer, Float> mapJ;
    public UserBasedCollabFiltering(SQLiteConnection sql) {
        this.sql = sql;
        sql.connect();
    }


    //The size of the batches before inserting into the DB
    public static final int BATCH_SIZE = 50000;

    public static final int PREDICTION_BATCH_SIZE = 10;



    public void calculateSimilarRated(HashMap<Integer,HashMap<Integer,Float>> map) {
        //Keeps count of the amount of similarities which have been successfully calculated
        int amountCalculated = 0;

        sql.startTransaction();

        int start_i = 1;
        int start_j = start_i;

        // Last record entered:
        ArrayList<Integer> lastRecord = sql.getLastRecordFromSimilarityTable(sql.UBCF);

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
                            //The calculation which concerns the item which the user j has rated negated by their average
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

                            //Regardless if there is a NaN exception this will execute and allow the transaction to end and it to be placed into the DB
                        } finally {
                            // Have 1000 items been calculated or has it reached the end of the table
                            if (amountCalculated % BATCH_SIZE == 0) {
                                sql.endTransaction();

                                    sql.startTransaction();


                            }
                        }
                    }
                    if(i == map.entrySet().size() - 1 && j == map.entrySet().size()) {
                        sql.endTransaction();
                    }
                }

            }
            start_j = i + 1; // Reset the counter for a new person

        }


    }


    // Works out predicted rating for two users. map represents the predictedSet
    public void calculatePredictedRating(HashMap<Integer,HashMap<Integer,Float>> map) {
        long amountCalculated = 0;
        int batch = 1;
        int averageCount = 0;
        //The amount of users which have been processed regardless if they have/haven't got any shared item similarities (cold start problem)
        long rowsProcessed = 0;
        long numberOfRows = sql.getAmountOfRows(SQLiteConnection.PREDICTED_RATING_TABLE, "userID");
        // Training set which can be used to find the items which the neighbours have rated
        HashMap<Integer,HashMap<Integer,Float>> trainingSet = sql.getTrainingSetToMemory(SQLiteConnection.TRAINING_SET);

        //Get averages in memory:
        HashMap<Integer, Float> averagesMap = sql.getAveragesToMemory();

        sql.startTransaction();

        for (int user: map.keySet()) {
            HashMap<Integer, Float> itemMap = map.get(user);
            for (int item: itemMap.keySet()) {
                HashMap<Integer, Float> neighbourMap = sql.getNeighbourSelection(user, item);
                float meanA = averagesMap.get(user);
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
                        top += similarity * (rating - averagesMap.get(userNew));
                        bottom += similarity;
                        neighbourhoodItemValid = true;

                    }
                }
                rowsProcessed++;

                if (neighbourhoodItemValid && neighbourMap.entrySet().size() >= 1) {

                    float rating = meanA + (top/bottom);
                    if(rating > 10) {
                        rating = 10; // Avoid float accuracy issues

                    } else if (rating < 1) {
                        rating = 1;

                    }
                    sql.insertPredictedRating(user, item, rating);
                    //Amount currently in the batch
                    amountCalculated++;
                } else {
                    // If the user doesn't have any suitable neighbours then we simply insert the average value given by that user
                    sql.insertPredictedRating(user, item, averagesMap.get(user));
                    amountCalculated++;
                    averageCount++;
                }
                if (amountCalculated % PREDICTION_BATCH_SIZE == 0 || (rowsProcessed == numberOfRows)) {
                    sql.endTransaction();
                    System.out.println("Batch: " + batch + " finished");

                    if (rowsProcessed != numberOfRows) {
                        sql.startTransaction();
                    }
                }
            }
        }

        System.out.println("Averages: " + averageCount);

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
