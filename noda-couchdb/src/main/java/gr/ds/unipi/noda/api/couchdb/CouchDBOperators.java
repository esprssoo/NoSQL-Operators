package gr.ds.unipi.noda.api.couchdb;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnector;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.operators.aggregateOperators.AggregateOperator;
import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;
import gr.ds.unipi.noda.api.core.operators.sortOperators.SortOperator;
import gr.ds.unipi.noda.api.couchdb.restClient.CouchDBCall;
import gr.ds.unipi.noda.api.couchdb.restClient.FindRequest;
import gr.ds.unipi.noda.api.couchdb.restClient.FindResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Optional;

final class CouchDBOperators extends NoSqlDbOperators {

    private final CouchDBConnectionManager couchDBConnectionManager = CouchDBConnectionManager.getInstance();
    private CouchDBCall<FindResult> pendingCall;

    private CouchDBOperators(NoSqlDbConnector noSqlDbConnector, String dataCollection, SparkSession sparkSession) {
        super(noSqlDbConnector, dataCollection, sparkSession);
    }

    static CouchDBOperators newCouchDBOperators(NoSqlDbConnector noSqlDbConnector, String dataCollection, SparkSession sparkSession) {
        return new CouchDBOperators(noSqlDbConnector, dataCollection, sparkSession);
    }

    @Override
    public NoSqlDbOperators filter(FilterOperator filterOperator, FilterOperator... filterOperators) {
        FindRequest findRequest = new FindRequest(filterOperator.getOperatorExpression());
        this.pendingCall = couchDBConnectionManager.getConnection(getNoSqlDbConnector())
                .find(getDataCollection(), findRequest);
        return this;
    }

    @Override
    public NoSqlDbOperators groupBy(String fieldName, String... fieldNames) {
        return null;
    }

    @Override
    public NoSqlDbOperators aggregate(AggregateOperator aggregateOperator, AggregateOperator... aggregateOperators) {
        return null;
    }

    @Override
    public NoSqlDbOperators distinct(String fieldName) {
        return null;
    }

    @Override
    public void printScreen() {

    }

    @Override
    public Optional<Double> max(String fieldName) {
        return Optional.empty();
    }

    @Override
    public Optional<Double> min(String fieldName) {
        return Optional.empty();
    }

    @Override
    public Optional<Double> sum(String fieldName) {
        return Optional.empty();
    }

    @Override
    public Optional<Double> avg(String fieldName) {
        return Optional.empty();
    }

    @Override
    public int count() {
        try {
            return this.pendingCall.execute().docs().size();
        } catch (IOException e) {
            System.out.println(e);
        }

        return 0;
    }

    @Override
    public NoSqlDbOperators sort(SortOperator sortOperator, SortOperator... sortingOperators) {
        return null;
    }

    @Override
    public NoSqlDbOperators limit(int limit) {
        return null;
    }

    @Override
    public NoSqlDbOperators project(String fieldName, String... fieldNames) {
        return null;
    }

    @Override
    public Dataset<Row> toDataframe() {
        return null;
    }
}
