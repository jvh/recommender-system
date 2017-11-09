import com.sun.xml.internal.bind.v2.TODO;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of the user-based collaborative filtering system given in the slides
 */
public class UserBasedCollabFiltering {

    SQLiteConnection sql = new SQLiteConnection();
    //The size of the batches before inserting into the DB
    public static final int BATCH_SIZE = 1000;

    public void calculateSimilarRated(HashMap<Integer,HashMap<Integer,Double>> map) {
        HashMap<Integer, HashMap<Integer, Double>> resultMap = new HashMap<>();

        HashMap<Integer, Double> currentUserRating;
        for (int i = 1; i < map.entrySet().size() ; i++) {
            currentUserRating = map.get(i);

            for (int j = i + 1 ; j < map.entrySet().size() ; j++) {
                HashMap<Integer, Double> mapJ = map.get(j);

                double topLine = 0;
                double userACalc = 0;
                double userBCalc = 0;
                boolean similarityExists = false;

                for (int key : mapJ.keySet()) {
                    if (currentUserRating.containsKey(key)) {
                        similarityExists = true;
                        System.out.println(i);
                        double first = currentUserRating.get(key) - sql.getUserAverage(i);
                        double second = mapJ.get(key) - sql.getUserAverage(j);

                        topLine += (first * second);
                        userACalc += Math.pow(first, 2);
                        userBCalc += Math.pow(second, 2);
                    }
                }
                if(similarityExists) {
                    double bottomLine = Math.sqrt(userACalc) * Math.sqrt(userBCalc);
                    double similarity = (topLine / bottomLine);


                    System.out.println("User A: " + i + " User B: " + j + " Similarity: " + similarity);
                }

            }

        }


    }


    //Works out the similarities between the users
    public void similarityMeasure() {
        sql.connect();

        //Stores the average for both of the users
        double userAAverage;
        double userBAverage;
        //The capacity being the size of the batches
        HashMap<String, Double> similaritiesToAdd = new HashMap<String, Double>(BATCH_SIZE);

        //Maximum number of iterations to create the similarity matrix
        final int MAX_ITERATIONS = sql.getAmountOfRows("testSet1");

        //Stores the items rated by 2 users.
        HashMap<Integer, String> similarItemsRated;


        for (int i = 1; i < MAX_ITERATIONS; i++) {
            userAAverage = sql.getUserAverage(i);
            for (int y = 1; y < MAX_ITERATIONS; y++) {
                userBAverage = sql.getUserAverage(y);

                //As to not produce duplicates in the similarity ratings table or to create similarities for the same users, i.e. i = 1, y = 1
                if (i <= y) {
                    //TODO This is selecting a value from the database each time, issue?
                    //Finds the items which both users have rated and the rating associated with them for both users.
                    similarItemsRated = sql.similarityValues(i, y);
                    //Finds the items which both users have rated and the rating associated with them for both users. Returns a hashmap
//                    similarItemsRated = sql.similarityValues(i, y);
//                    HashMap<Integer, String> similarItemsRated = new HashMap<Integer, String>();


                    double topLine = 0;
                    double userACalc = 0;
                    double userBCalc = 0;
                    double similarity = 0;

                    if (similarItemsRated.entrySet().size() > 1) {
                        for (HashMap.Entry<Integer, String> entry : similarItemsRated.entrySet()) {
                            //Ratings returned for both users for the same item
                            double ratingA = Double.parseDouble(entry.getValue().split(",")[0]);
                            double ratingB = Double.parseDouble(entry.getValue().split(",")[1]);

                            double first = ratingA - userAAverage;
                            double second = ratingB - userBAverage;

                            topLine += (first * second);
                            userACalc += Math.pow(first, 2);
                            userBCalc += Math.pow(second, 2);
                        }
                        double bottomLine = Math.sqrt(userACalc) * Math.sqrt(userBCalc);

                        similarity = (topLine / bottomLine);

                        //Add to the similarities table only if we have not reached the end of the users
                        if (i <= MAX_ITERATIONS && y <= MAX_ITERATIONS && !Double.isNaN(similarity)) {
                            similaritiesToAdd.put(i + "," + y + "," + similarItemsRated.size(), similarity);
                            similarItemsRated.clear();
                        }

                        if (similaritiesToAdd.size() % 1000 == 0 || (i == MAX_ITERATIONS & y == MAX_ITERATIONS)){
                            System.out.println("Starting batch");
                            for (HashMap.Entry<String, Double> entry : similaritiesToAdd.entrySet()) {
                                int userA = Integer.parseInt(entry.getKey().split(",")[0]);
                                int userB = Integer.parseInt(entry.getKey().split(",")[1]);
                                int similarItems = Integer.parseInt(entry.getKey().split(",")[2]);
                                double similarityRating = entry.getValue();

//                            System.out.println(userA + ", userB: " + userB + " y: " + y + ", similarity: " + similarity + ", similarItem: " + similarItems);

                                sql.insertSimilarityValue(userA, userB, similarityRating, similarItems);
                            }
                            similaritiesToAdd.clear();
                            System.out.println("DONE BATCH");
//                        if(i < MAX_ITERATIONS && y < MAX_ITERATIONS) {
//                            System.out.println("*********************DONE STACK*************************");
////                            similarityMeasure(i, y);
//                        } else {
//                            System.out.println("FINISHED");
//                            return;
//                        }
                        }

                    }
//                    System.out.println("i= " + i + " y= " + y);
                    //Batch processing (insertion)


                }
            }
        }
    }

    // Works out predicted rating for two users
    public void calculatePredictedRating(int userA, int itemB) {
        sql.connect();
        //TODO Needs to check if user has actually rated item, if so don't calculate
        HashMap<Integer, Double> neighbourMap = sql.getNeighbourSelection(userA);
        double meanA = sql.getUserAverage(userA);
        Double top = 0.0;
        Double bottom = 0.0;
        for(HashMap.Entry<Integer, Double> entry : neighbourMap.entrySet()) {
            int userNew = entry.getKey();
            double similarity = entry.getValue();
            top += similarity * (sql.getNeighbourhoodRated(userNew, itemB) - sql.getUserAverage(userNew));
            bottom += similarity;
        }
        double rating = meanA + (top/bottom);
//        System.out.println(meanA + (top/bottom));
        sql.insertPredictedRating(userA, itemB, rating, "testSet1");
        sql.closeConnection();

    }

}
