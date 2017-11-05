import java.util.HashMap;

/**
 * Created by jh47g15 on 18/10/17.
 */
public class Main {

    public static void main(String[] args) {
        UserBasedCollabFiltering ubcf = new UserBasedCollabFiltering();
//        ubcf.similarityMeasure();

        ubcf.calculatePredictedRating(1, 20919);


//        Main m = new Main();

//        m.hashWithinHashTest();

    }

    private void hashWithinHashTest() {
        HashMap<Integer, HashMap<Integer, Integer>> hm = new HashMap<Integer, HashMap<Integer, Integer>>();
        HashMap<Integer, Integer> hm2 = new HashMap<Integer, Integer>();

        //user 1 ratings
        hm2.put(2, 5);
        hm2.put(3, 7);

        HashMap<Integer, Integer> hm3 = new HashMap<Integer, Integer>();


        hm.put(1, hm2);
        //user 2 ratings
        hm3.put(3, 8);
        hm.put(2, hm3);

            HashMap<Integer, Integer> hmtest = new HashMap<Integer, Integer>();

            hmtest.put(1, 2);
            hm.put(5, hmtest);

        hmtest = new HashMap<Integer, Integer>();

        hmtest.put(2, 9);

        hm.put(5, hmtest);


        hmtest = new HashMap<Integer, Integer>();

            hmtest.put(2, 3);

            hm.put(6, hmtest);


        for(int i = 0; i < hm.entrySet().size(); i++) {
            System.out.println(hm.get(i));

        }
    }
}
