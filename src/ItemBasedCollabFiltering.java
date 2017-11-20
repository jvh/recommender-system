import java.util.ArrayList;
import java.util.HashMap;

public class ItemBasedCollabFiltering {

    SQLiteConnection sql;
    HashMap<Integer, Float> currentUserRating;
    HashMap<Integer, Float> mapJ;
    public ItemBasedCollabFiltering(SQLiteConnection sql) {
        this.sql = sql;
        sql.connect();
    }


    //The size of the batches before inserting into the DB
    public static final int BATCH_SIZE = 50000;

    public static final int PREDICTION_BATCH_SIZE = 1000;

    public void calculateSimilarity(HashMap<Integer,HashMap<Integer,Float>> map) {
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
//                            System.err.println("user A: " + i + " user B: " + j + " has encountered a NaN exception");

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

}
