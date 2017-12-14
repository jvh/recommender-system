import java.util.ArrayList;
import java.util.HashMap;

public class ItemBasedCollabFiltering extends CollabFiltering {

    SQLiteConnection sql;
    HashMap<Integer, Float> itemIUserMap;
    HashMap<Integer, Float> itemJUserMap;
    public ItemBasedCollabFiltering(SQLiteConnection sql) {
        this.sql = sql;
        sql.connect();
    }


    //The size of the batches before inserting into the DB
    public static final int BATCH_SIZE = 50000;

    public static final int PREDICTION_BATCH_SIZE = 100;

    //How much leniency should be given to the alternative to averages (in predictions)
    public static final double PREDICTIONS_LENIENCY = 1;


    public void calculateSimilarity(HashMap<Integer,HashMap<Integer,Float>> map) {
        //Keeps count of the amount of similarities which have been successfully calculated
        int amountCalculated = 0;

        sql.startTransaction();

        int start_i = 1;
        int start_j = start_i;

        // Last record entered:
        ArrayList<Integer> lastRecord = sql.getLastRecordFromSimilarityTable(sql.IBCF);

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

            //These are the users which have rated item i
            itemIUserMap = map.get(i);

            // Has the item been rated by any users
            if (itemIUserMap.size() > 0) {
                for (int j = start_j + 1; j <= map.entrySet().size(); j++) {
                    //Users which have rated item j
                    itemJUserMap = map.get(j);

                    float topLine = 0;
                    float userACalc = 0;
                    float userBCalc = 0;
                    //There is a similarity between the 2 items (at least one user has rated both)
                    boolean similarityExists = false;
                    //Number of users which have rated both items
                    int similarUsers = 0;


                    for (int key : itemJUserMap.keySet()) {
                        if (itemIUserMap.containsKey(key)) {

                            similarUsers++;
                            similarityExists = true;

                            //The calculation which concerns the the user's rating for item i, negated by their [the users] average
                            float first = itemIUserMap.get(key) - averagesMap.get(key);

//                          //The calculation which concerns the the user's rating for item j, negated by their [the users] average
                            float second = itemJUserMap.get(key) - averagesMap.get(key);

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
                                sql.insertSimilarityMeasure(i, j, similarity, similarUsers);

                            }

                        } catch (NumberFormatException e) {
//                            System.err.println("user A: " + i + " user B: " + j + " has encountered a NaN exception");

                            //Regardless if there is a NaN exception this will execute and allow the transaction to end and it to be placed into the DB
                        } finally {
                            // Have 1000 items been calculated or has it reached the end of the table
                            if (amountCalculated % BATCH_SIZE == 0) {
                                sql.endTransaction();

                                //Start a new batch
                                sql.startTransaction();
                            }
                        }
                    }
                    if((i == map.entrySet().size() - 1 && j == map.entrySet().size())) {
                        sql.endTransaction();
                    }
                }

            }
            start_j = i + 1; // Reset the counter for a new person

        }


    }

    // Works out predicted rating for a user and corresponding item.. map represents the predictedSet
    public void calculatePredictedRating(HashMap<Integer, HashMap<Integer, Float>> map) {
        long amountCalculated = 0;
        int batch = 1;
        //The number of items rated regardless if they have been rated by the same user (cold start problem)
        long rowsProcessed = 0;
        long numberOfRows = sql.getAmountOfRows(SQLiteConnection.PREDICTED_RATING_TABLE, "itemID");
        // Training set which can be used to find the users which have rated the same item
        HashMap<Integer,HashMap<Integer,Float>> trainingSet = sql.getTrainingSetToMemoryIBCF(SQLiteConnection.TRAINING_SET);

        //Get averages in memory:
        HashMap<Integer, Float> averagesMap = sql.getAveragesToMemory();

        sql.startTransaction();
        int averageCount = 0;

        for (int item: map.keySet()) {
            HashMap<Integer, Float> userList = map.get(item);
            for (int user : userList.keySet()) {
                HashMap<Integer, Float> neighbourItemMap = sql.getNeighbourSelectionItemBased(item, user);
                float top = 0.0f;
                float bottom = 0.0f;
                boolean neighbourhoodItemValid = false;

                //In the neighbourhood find the users which have rated both the items (item and itemNew)
                for(HashMap.Entry<Integer, Float> entry : neighbourItemMap.entrySet()) {
                    //Item which we're comparing the original item to
                    int itemNew = entry.getKey();
                    HashMap<Integer, Float> userNewItemRatings = trainingSet.get(itemNew);


                    // Has the neighbourhood item been rated by the same user
                    if (userNewItemRatings.containsKey(user)) {

                        // Rating which the user has given itemNew
                        float rating = userNewItemRatings.get(user);

                        float similarity = entry.getValue();
                        top += similarity * rating;
//                        top += similarity * (sql.getNeighbourhoodRated(userNew, user) - sql.getUserAverage(userNew));
                        bottom += similarity;
                        neighbourhoodItemValid = true;

                    } else {
                        System.out.println("don't");
                    }
                }
                rowsProcessed++;

                if (neighbourhoodItemValid && neighbourItemMap.entrySet().size() >= 1) {
                    float rating = (top/bottom);
                    float decimalPart = rating % 1;
//                    System.out.println("rating: " + rating + " user: " + user + " item: " + item);
                    if(rating > 10) {
                        rating = 10; // Avoid float accuracy issues

                    } else if (rating < 1) {
                        rating = 1;

                    }

//                    } else if (decimalPart <= 0.3) {
//                        rating = Math.round(rating);
//
//                    } else if (decimalPart >= 0.7) {
//                        rating = Math.round(rating);
//                    }
//                    System.out.println("rating: " + rating + " user: " + user + " item: " + item);
                    sql.insertPredictedRating(user, item, rating);
                    //Amount currently in the batch
                    amountCalculated++;
                } else {
//                    int totalRating = 0;
//                    int count = 0;
//                    //Get the average rating given to the item by all users
//                    for(float rating: trainingSet.get(item).values()) {
//                        count++;
//                        totalRating += rating;
//                    }

                    //****TESTING****
//                    float actualRating = trainingSet.get(item).get(user);

//                    float rating = (float) totalRating/count;
                    float averageRating = averagesMap.get(user);

//                    //If the rating is above the leniency given, just input the average rating
//                    if(Math.abs(rating-averageRating) >= PREDICTIONS_LENIENCY) {
//                        sql.insertPredictedRating(user, item, averageRating);
//                    } else {
//                        sql.insertPredictedRating(user, item, rating);
//                    }

                    // If the item doesn't have any suitable neighbours then we simply insert the average value given by that user
                    sql.insertPredictedRating(user, item, averageRating);
                    amountCalculated++;
                    averageCount++;
                }

                if (amountCalculated % PREDICTION_BATCH_SIZE == 0 || (rowsProcessed == numberOfRows)) {
                    sql.endTransaction();
                    System.out.println("Batch: " + batch + " finished");

                    if (rowsProcessed != numberOfRows) {
                        sql.startTransaction();
                        batch++;
                    }
                }
            }
        }
        System.out.println("Averages: " + averageCount);

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

}
