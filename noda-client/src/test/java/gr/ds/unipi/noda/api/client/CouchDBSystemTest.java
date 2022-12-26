package gr.ds.unipi.noda.api.client;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import org.junit.Test;

import static gr.ds.unipi.noda.api.core.operators.AggregateOperators.avg;
import static gr.ds.unipi.noda.api.core.operators.AggregateOperators.sum;
import static gr.ds.unipi.noda.api.core.operators.FilterOperators.gte;

public class CouchDBSystemTest {
    private NoSqlDbOperators getOperators(String db) {
        return NoSqlDbSystem.CouchDB().Builder("admin", "password").host("localhost").port(5984).build().operateOn(db);
    }

    @Test
    public void couchdbTest() {
        NoSqlDbOperators noSqlDbOperators = getOperators("animals");

//        System.out.println(noSqlDbOperators.filter(gte("weight", 100)).sum("weight"));
        noSqlDbOperators.filter(gte("weight", 0))
                .aggregate(sum("weight"), avg("height").as("average_height"))
                .groupBy("species")
                .printScreen();
//        noSqlDbOperators.filter(gte("weight", 100)).sort(asc("height")).printScreen();
//        noSqlDbOperators.filter(gte("weight", 100)).sort(desc("height")).printScreen();
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
