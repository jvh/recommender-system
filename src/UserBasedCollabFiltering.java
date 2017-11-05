import com.sun.xml.internal.bind.v2.TODO;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of the user-based collaborative filtering system given in the slides
 */
public class UserBasedCollabFiltering {

    SQLiteConnection sql = new SQLiteConnection();


    //Works out the similarities between the users
    public void similarityMeasure(int num1, int num2) {
        sql.connect();
        //Stores the average for both of the users
        double userAAverage;
        double userBAverage;
        HashMap<String, Double> similaritiesToAdd = new HashMap<String, Double>(1000);

        //Maximum number of iterations to create the similarity matrix
        int maxIterations = sql.getAmountOfRows("testSet1");
        //Number of batches needed
        double numberBatches = Math.ceil(maxIterations/1000.0);
        System.out.println("maxIterations: " + maxIterations + ", numberBatches: " + numberBatches);

        HashMap<Integer, String> similarItemsRated;

        for (int i = num1; i < maxIterations; i++) {
            userAAverage = sql.getUserAverage(i);
            for (int y = num2; y < maxIterations; y++) {
                userBAverage = sql.getUserAverage(y);

                if (i != y) {

                    //TODO This is selecting a value from the database each time, issue?
                    //Finds the items which both users have rated and the rating associated with them for both users. Returns a hashmap
                    similarItemsRated = sql.similarityValues(i, y);
//                    HashMap<Integer, String> similarItemsRated = new HashMap<Integer, String>();


                    long startTime1 = System.currentTimeMillis();


                    //Stores the calculation for the top line of the similarity measure equation
                    double topLine = 0;
                    //Stores the calculations for future use
                    double userACalc = 0;
                    double userBCalc = 0;
                    //Similarity between the 2 users
                    double similarity = 0;


                    if (similarItemsRated.entrySet().size() > 1) {
                        for (HashMap.Entry<Integer, String> entry : similarItemsRated.entrySet()) {
                            int itemID = entry.getKey();
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

                        if (i < maxIterations && y < maxIterations && !Double.isNaN(similarity)) {
                            similaritiesToAdd.put(i + "," + y + "," + similarItemsRated.size(), similarity);
                            similarItemsRated.clear();
//                        System.out.println(similaritiesToAdd.size());
//                            System.out.println("*********************ADDING************************");
                        }
                        if (similaritiesToAdd.size() % 1000 == 0){
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
//                        if(i < maxIterations && y < maxIterations) {
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
