package gr.ds.unipi.noda.api.client;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import org.junit.Test;

import static gr.ds.unipi.noda.api.core.operators.AggregateOperators.sum;
import static gr.ds.unipi.noda.api.core.operators.FilterOperators.*;
import static gr.ds.unipi.noda.api.core.operators.SortOperators.desc;

public class CouchDBSystemTest {
    private NoSqlDbSystem getSystem() {
        return NoSqlDbSystem.CouchDB().Builder("admin", "password").host("localhost").port(5984).build();
    }

    @Test
    public void couchdbTest() {
        getSystem().operateOn("animals")
                .groupBy("species")
//                .aggregate(sum("weight"))
                .project("height", "weight", "name")
                .printScreen();
    }

    @Test
    public void simpleFilterTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("animals").filter(gte("weight", 100)).printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Test
    public void simpleGroupByTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("animals").groupBy("species").printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Test
    public void groupByCombinedWithAggregationTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("animals").groupBy("species").aggregate(sum("weight")).printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Test
    public void logicalOperatorTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("animals").filter(and(gte("weight", 200), lte("weight", 500))).printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Test
    public void stucturalSharingTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("animals");


        NoSqlDbOperators var = noSqlDbOperators.filter(and(gte("weight", 200), lte("weight", 500)));
        var = noSqlDbOperators.groupBy("weight");
        var.printScreen();

        noSqlDbSystem.closeConnection();
    }
}
