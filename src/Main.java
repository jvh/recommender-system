import java.util.HashMap;

/**
 * Created by jh47g15 on 18/10/17.
 */
public class Main {

    public static void main(String[] args) {
        UserBasedCollabFiltering ubcf = new UserBasedCollabFiltering();



    }

    private void hashWithinHashTest() {
        HashMap<Integer, HashMap<Integer, Integer>> hm = new HashMap<Integer, HashMap<Integer, Integer>>();
        HashMap<Integer, Integer> hm2 = new HashMap<Integer, Integer>();

        hm2.put(2, 5);
        hm2.put(3, 7);

        hm2.clear();

        hm2.put(3, 8);

        hm.put(1, hm2);
        hm.put(2, hm2);

        System.out.println(hm.get(2));
    }
}
