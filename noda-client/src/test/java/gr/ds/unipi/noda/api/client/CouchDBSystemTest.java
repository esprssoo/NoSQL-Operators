package gr.ds.unipi.noda.api.client;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import org.junit.Test;

import static gr.ds.unipi.noda.api.core.operators.FilterOperators.gte;

public class CouchDBSystemTest {
    @Test
    public void couchdbTest() {
        NoSqlDbSystem noSqlDbSystem = NoSqlDbSystem.CouchDB().Builder("admin", "password").host(
                "localhost").port(5984).build();

        NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("animals");

        //        Optional<Double> maxWeight = noSqlDbOperators.filter(lte("weight", 500)).sum("weight");
        //        System.out.println(maxWeight.isPresent() ? maxWeight.get() : 0);

        //        noSqlDbOperators.filter(lte("weight", 500)).limit(1).printScreen();
        //        System.out.println(noSqlDbOperators.filter(lte("weight", 500)).groupBy("name").count());

        noSqlDbOperators.filter(gte("weight", 100)).printScreen();

        noSqlDbSystem.closeConnection();
    }
}
