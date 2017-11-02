import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of the user-based collaborative filtering system given in the slides
 */
public class UserBasedCollabFiltering {

    SQLiteConnection sql = new SQLiteConnection();
    final static int MAX_ITERATIONS = 1000;

    //Works out the similarities between the users
    public void similarityMeasure(int num1, int num2) {
        sql.connect();
        //Stores the average for both of the users
        double userAAverage;
        double userBAverage;
        HashMap<String, Double> similaritiesToAdd = new HashMap<String, Double>();

        for (int i = num1; i < MAX_ITERATIONS; ++i) {
            userAAverage = sql.getUserAverage(i);
            for (int y = num2; y < MAX_ITERATIONS; ++y) {
                userBAverage = sql.getUserAverage(y);

                if (i == y) {
                    similaritiesToAdd.put(i + "," + y + 0, 0.0);
                } else {
                    //Finds the items which both users have rated and the rating associated with them for both users. Returns a hashmap
                    HashMap<Integer, String> similarItemsRated = sql.similarityValues(i, y);
//                    HashMap<Integer, String> similarItemsRated = new HashMap<Integer, String>();

                    //Stores the calculation for the top line of the similarity measure equation
                    double topLine = 0;
                    //Stores the calculations for future use
                    double userACalc = 0;
                    double userBCalc = 0;

                    if (similarItemsRated.entrySet().size() > 0) {
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

                        double similarity = (topLine / bottomLine);


                    } else {
                        similaritiesToAdd.put(i + "," + y + "," + 0, 0.0);
                    }

                    //Batch processing (insertion)
                    if (similaritiesToAdd.entrySet().size() <= 1000 && i < MAX_ITERATIONS && y < MAX_ITERATIONS) {
                        similaritiesToAdd.put(i + "," + y + "," + similarItemsRated.size(), similarity);
                        System.out.println(similaritiesToAdd.size());
//                            System.out.println("*********************ADDING************************");
                    } else {
                        System.out.println(similaritiesToAdd.size());
                        for (HashMap.Entry<String, Double> entry : similaritiesToAdd.entrySet()) {
                            int userA = Integer.parseInt(entry.getKey().split(",")[0]);
                            int userB = Integer.parseInt(entry.getKey().split(",")[1]);
                            int similarItems = Integer.parseInt(entry.getKey().split(",")[2]);
                            double similarityRating = entry.getValue();

                            sql.insertSimilarityValue(userA, userB, similarityRating, similarItems);
                        }
                        System.out.println("*********************DONE STACK*************************");
                        if(i < MAX_ITERATIONS && y < MAX_ITERATIONS) {
                            System.out.println("recurse");
                            similarityMeasure(i, y);
                        } else {
                            System.out.println("FINISHED");
                            return;
                        }
                    }

                }
            }
        }

    }
}
