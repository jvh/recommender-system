import java.util.ArrayList;
import java.util.HashMap;

public class SlopeOne {

    SQLiteConnection sql;

    public SlopeOne(SQLiteConnection sql) {
        this.sql = sql;
    }

    //Calculates the average differences between pairs of items
    protected void calculateAverageDifferences(HashMap<Integer, HashMap<Integer, Float>> map) {
        //The map shall be the map used in item-based, i.e. <itemID, <userID, rating>>

        //Keeps count of the amount of similarities which have been successfully calculated
        int amountCalculated = 0;

        sql.startTransaction();

        int start_i = 1;
        int start_j = start_i;

        // Last record entered:
        ArrayList<Integer> lastRecord = sql.getLastRecordFromSimilarityTable(sql.UBCF);

        //Get averages in memory:
        HashMap<Integer, Float> averagesMap = sql.getAveragesToMemory();

        if (!lastRecord.isEmpty()) {
            if (lastRecord.get(1) + 1 > map.entrySet().size()) { // If its the last entry for userA
                start_i++;
                start_j = start_i;

            } else {
                start_i = lastRecord.get(0);
                start_j = lastRecord.get(1);
            }

        }

        HashMap<Integer, Float> mapI;
        HashMap<Integer, Float> mapJ;

        for (int i = start_i; i < map.entrySet().size() ; i++) {

            //These are the users which have rated the item
            mapI = map.get(i);

            // Has the user rated some item(s)
            if (mapI.size() > 0) {
                for (int j = start_j + 1; j <= map.entrySet().size(); j++) {
                    mapJ = map.get(j);

                    int countSimilarUsers = 0;
                    boolean similarityExists = false;
                    int topLine = 0;

                    for (int key : mapJ.keySet()) {
                        if (mapI.containsKey(key)) {
                            countSimilarUsers++;
                            similarityExists = true;

                            topLine += (mapI.get(key) - mapJ.get(key));
                        }
                    }

                    if (similarityExists) {

                                float difference = topLine / countSimilarUsers;
                                amountCalculated++;
                                // Insert one value into the transaction block to insert
                                sql.insertAverageDifferences(i, j, difference, countSimilarUsers);


                            // Have 1000 items been calculated or has it reached the end of the table
                            if (amountCalculated % UserBasedCollabFiltering.BATCH_SIZE == 0) {
                                sql.endTransaction();

                                sql.startTransaction();
                            }
                        }

                    if(i == map.entrySet().size() - 1 && j == map.entrySet().size()) {
                        sql.endTransaction();
                    }
                }
            }
            start_j = i + 1; // Reset the counter for a new person
        }
    }
}
