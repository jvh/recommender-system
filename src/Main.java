/**
 * Created by jh47g15 on 18/10/17.
 */
public class Main {

    public static void main(String[] args) {
        SQLiteConnection sql = new SQLiteConnection();
        UserBasedCollabFiltering ubcf = new UserBasedCollabFiltering(sql);
//        ubcf.calculateSimilarRated(sql.getTrainingSetToMemory("testSetSmallUnix"));
//        ubcf.calculatePredictedRating(sql.getTrainingSetToMemory("testSetSmallUnix"));
        ubcf.calculateSimilarRated(sql.getTrainingSetToMemory("trainingSet"));
        //  Testing out if the similarity measure works
//        ubcf.calculatePredictedRating(sql.getTrainingSetToMemory("predictedSmallSet"), "testSetSmallUnix");
        sql.closeConnection();
    }

}
