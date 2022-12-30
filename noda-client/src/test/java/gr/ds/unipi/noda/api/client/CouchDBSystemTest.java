package gr.ds.unipi.noda.api.client;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import org.junit.Test;

import static gr.ds.unipi.noda.api.core.operators.FilterOperators.*;
import static gr.ds.unipi.noda.api.core.operators.SortOperators.desc;

public class CouchDBSystemTest {
    private NoSqlDbOperators getOperators(String db) {
        return NoSqlDbSystem.CouchDB().Builder("admin", "password").host("localhost").port(5984).build().operateOn(db);
    }

    @Test
    public void couchdbTest() {
        NoSqlDbOperators noSqlDbOperators = getOperators("animals");

        noSqlDbOperators.filter(or(gte("weight", 100), lt("height", 10)))
                .filter(ne("name", "Rabbit"))
                .sort(desc("height"), desc("weight"))
                .printScreen();

//        noSqlDbOperators.filter(gte("weight", 0))
//                .filter(eq("name", "Tiger"))
//                .aggregate(sum("weight"), avg("height").as("average_height"))
//                .groupBy("species")
//                .printScreen();
    }

    @Test
    public void countWorks() {
        NoSqlDbOperators noSqlDbOperators = getOperators("animals");

        long startTime = System.currentTimeMillis();
        int count = noSqlDbOperators.count();
        long elapsed = System.currentTimeMillis() - startTime;

        assert count == 4;
        System.out.println("countWorks() took " + elapsed + "ms");
    }
}
