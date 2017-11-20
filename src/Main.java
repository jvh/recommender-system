/**
 * Created by jh47g15 on 18/10/17.
 */
public class Main {

    public static void main(String[] args) {
        SQLiteConnection sql = new SQLiteConnection();
        UserBasedCollabFiltering ubcf = new UserBasedCollabFiltering(sql);

        //STEP 1: Computes the average for all given users
//        ubcf.computeAllAverages(sql.getTrainingSetToMemory(SQLiteConnection.TRAINING_SET));

        //STEP 2: Computes the similarities between the users from the trainingSet
//        ubcf.calculateSimilarRated(sql.getTrainingSetToMemory(SQLiteConnection.TRAINING_SET));

        //STEP 3: Computes the predicted ratings for the users given the similarities
        ubcf.calculatePredictedRating(sql.getPredictionSetToMemory(SQLiteConnection.PREDICTED_RATING_TABLE));

        sql.closeConnection();
    }

}
