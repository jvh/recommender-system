
public class Main {

    public static void main(String[] args) {
        SQLiteConnection sql = new SQLiteConnection();
        UserBasedCollabFiltering ubcf = new UserBasedCollabFiltering(sql);
//        ItemBasedCollabFiltering ibcf = new ItemBasedCollabFiltering(sql);

        //STEP 1: Computes the average for all given users
//        ubcf.computeAllAverages(sql.getTrainingSetToMemory(SQLiteConnection.TRAINING_SET));

        //STEP 2: Computes the similarities between the users from the trainingSet
//        ubcf.calculateSimilarRated(sql.getTrainingSetToMemory(SQLiteConnection.TRAINING_SET));
//        ibcf.calculateSimilarity(sql.getTrainingSetToMemoryIBCF(SQLiteConnection.TRAINING_SET));

        //STEP 3: Computes the predicted ratings for the users given the similarities
//        ibcf.calculatePredictedRating(sql.getPredictionSetToMemory(SQLiteConnection.PREDICTED_RATING_TABLE));
//        ubcf.calculatePredictedRating(sql.getPredictionSetToMemory(SQLiteConnection.PREDICTED_RATING_TABLE));

        //Calculate mean square error:
//         System.out.println(ibcf.calculateMeanSquareError(sql.getTrainingSetToMemoryIBCF(sql.TRAINING_SET), sql.getPredictionSetToMemory(sql.PREDICTED_RATING_TABLE)));
        System.out.println(ubcf.calculateMeanSquareError(sql.getTrainingSetToMemory(sql.TRAINING_SET), sql.getPredictionSetToMemory(sql.PREDICTED_RATING_TABLE)));

        sql.closeConnection();
    }

}
