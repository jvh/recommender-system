import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implementation of the user-based collaborative filtering system given in the slides
 */
public class UserBasedCollabFiltering {

    SQLiteConnection sql = new SQLiteConnection();

    //Works out the similarities between the users
    public void simuilarityMeasure() {
        //Stores the average for both of the users
        double userAAverage;
        double userBAverage;

        for(int i = 0; i < 9; ++i) {
            i = 49;
            userAAverage = sql.getUserAverage(i);
            for(int y = 0; y < 9; ++y) {
                y =124;
                userBAverage = sql.getUserAverage(y);

                if(i == y) {
//                    sql.addSimilarity(i, y, 0);
                } else {
                    //Finds the items which both users have rated and the rating associated with them for both users. Returns a hashmap
                    HashMap<Integer, String> similarItemsRated = sql.similarityValues(i, y);
//                    HashMap<Integer, String> similarItemsRated = new HashMap<Integer, String>();

                    //Stores the calculation for the top line of the similarity measure equation
                    double topLine = 0;
                    //Stores the calculations for future use
                    double userACalc = 0;
                    double userBCalc = 0;


                    for(HashMap.Entry<Integer, String> entry: similarItemsRated.entrySet()) {
                        int itemID = entry.getKey();
                        //Ratings returned for both users for the same item
                        int ratingA = Integer.parseInt(entry.getValue().split(" ")[0]);
                        int ratingB = Integer.parseInt(entry.getValue().split(" ")[1]);

                        double first = ratingA - userAAverage;
                        double second = ratingB - userBAverage;

                        topLine = topLine + (first * second);
                        userACalc = userACalc + first;
                        userBCalc = userBCalc + second;
                    }

                    double firstSqrRt = Math.sqrt(Math.pow(userACalc, 2));
                    double seocndSqrRt = Math.sqrt(Math.pow(userBCalc, 2));

                    double bottomLine = firstSqrRt * seocndSqrRt;

                    double similarity = topLine / bottomLine;

                    sql.insertSimilarityValue(i, y, similarity);
                }
            }
        }
    }


}
