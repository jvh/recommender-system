import java.util.HashMap;


public class CollabFiltering {

    //Calculates the mean squared error of the predicted data and the true values
    public Double calculateMeanSquareError(HashMap<Integer, HashMap<Integer, Float>> realValueSet, HashMap<Integer, HashMap<Integer, Float>> predictedValueSet) {
        int count = 0;
        float squareError = 0.0f;

        // Key can be either item or user
        for(Integer key1 : predictedValueSet.keySet()) {
            HashMap<Integer, Float> predictedValues = predictedValueSet.get(key1);
            for (Integer key2 : predictedValues.keySet()) {
                float predictedValue = predictedValues.get(key2);
                float realValue = realValueSet.get(key1).get(key2);
                squareError += Math.pow(predictedValue - realValue, 2);
                count++; // This is the value of n in the equation
            }
        }
        double rootMeanSquareError = squareError/count;
        return rootMeanSquareError;
    }
}
