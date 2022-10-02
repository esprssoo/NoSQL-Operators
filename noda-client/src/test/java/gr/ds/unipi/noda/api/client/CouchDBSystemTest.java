package gr.ds.unipi.noda.api.client;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static gr.ds.unipi.noda.api.core.operators.FilterOperators.*;

public class CouchDBSystemTest {

    @Test
    public void couchDbTest() {
        NoSqlDbSystem noSqlDbSystem = NoSqlDbSystem.CouchDB()
                .Builder("admin", "password", "")
                .host("localhost")
                .port(5984)
                .build();

        NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("animals");

        System.out.println(noSqlDbOperators.filter(gte("weight", 151)).count());

        System.out.println(noSqlDbOperators.filter(eq("weight", 300)).count());

        System.out.println(noSqlDbOperators.filter(or(gte("weight", 300), eq("weight", 150)))
                                   .count());

        noSqlDbSystem.closeConnection();
    }

}
