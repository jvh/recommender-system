/**
 * Created by jh47g15 on 18/10/17.
 */

import java.util.HashMap;

/**
 * Modelling a user
 */
public class User {

    private int averageRating;

    //The ratings the user has received from other users
    private HashMap<Integer, Integer> usersRated = new HashMap<Integer, Integer>();

    private int userID;


    protected User(int userID) {
        this.userID = userID;
    }

    protected void addUserRating(int itemID, int rating) {
        usersRated.put(itemID, rating);
    }

    protected HashMap<Integer, Integer> getRatings() {
        return usersRated;
    }

  /*  protected int average() {

    }*/

    /*protected int getAverage() {
        return average();
    }*/
}