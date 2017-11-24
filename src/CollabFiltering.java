import java.util.HashMap;


public class CollabFiltering {

    public float calculateMeanSquareError(HashMap<Integer, HashMap<Integer, Float>> realValueSet, HashMap<Integer, HashMap<Integer, Float>> predictedValueSet) {
        float rootMeanSquareError = 0.0f;
        int count = 0;
        float squareError = 0.0f;

        // Key can be either item or user
        for(Integer key1 : realValueSet.keySet()) {
            HashMap<Integer, Float> realValues = realValueSet.get(key1);
            for (Integer key2 : realValues.keySet()) {
                Float realValue = realValueSet.get(key1).get(key2);
                Float predictedValue = predictedValueSet.get(key1).get(key2);
                squareError += Math.pow(predictedValue - realValue, 2);
                count++; // This is the value of n in the equation
            }
        }

        rootMeanSquareError += Math.sqrt((1/count) * squareError);
        return rootMeanSquareError;
    }


}
