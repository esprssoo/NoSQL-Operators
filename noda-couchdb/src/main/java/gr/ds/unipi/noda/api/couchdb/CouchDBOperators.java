package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.GsonBuilder;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.operators.aggregateOperators.AggregateOperator;
import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;
import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;
import gr.ds.unipi.noda.api.core.operators.sortOperators.SortOperator;
import gr.ds.unipi.noda.api.couchdb.objects.ViewResponse;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class CouchDBOperators extends NoSqlDbOperators {

    private final CouchDBConnectionManager couchDBConnectionManager = CouchDBConnectionManager.getInstance();
    private final CouchDBView.Builder viewBuilder;

    private CouchDBOperators(CouchDBConnector connector, String dataCollection, SparkSession sparkSession) {
        super(connector, dataCollection, sparkSession);
        viewBuilder = new CouchDBView.Builder().database(dataCollection);
    }

    private CouchDBOperators(CouchDBOperators self, CouchDBView.Builder viewBuilder) {
        super(self.getNoSqlDbConnector(), self.getDataCollection(), self.getSparkSession());
        this.viewBuilder = viewBuilder;
    }

    static CouchDBOperators newCouchDBOperators(CouchDBConnector noSqlDbConnector, String dataCollection, SparkSession sparkSession) {
        return new CouchDBOperators(noSqlDbConnector, dataCollection, sparkSession);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators filter(FilterOperator filterOperator, FilterOperator... filterOperators) {
        // Combines multiple filter operators with logical condition And
        String filter = Stream.concat(Stream.of(filterOperator), Stream.of(filterOperators))
                .flatMap(o -> Stream.of((StringBuilder) o.getOperatorExpression(), "&&"))
                .limit(filterOperators.length * 2L + 1)
                .collect(Collectors.joining());

        return new CouchDBOperators(this, new CouchDBView.Builder().database(getDataCollection()).filter(filter));
    }

    @Override
    public CouchDBOperators groupBy(String fieldName, String... fieldNames) {
        viewBuilder.groupFields(Stream.concat(Stream.of(fieldName), Stream.of(fieldNames)).collect(Collectors.toSet()));
        return this;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators aggregate(AggregateOperator aggregateOperator, AggregateOperator... aggregateOperators) {
        return this;
    }

    @Override
    public CouchDBOperators distinct(String fieldName) {
        viewBuilder.group(true).reduce("function() { return true }");
        return groupBy(fieldName);
    }

    @Override
    public void printScreen() {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());
        ViewResponse response = connection.execute(viewBuilder.build());
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(response));
    }

    @Override
    public Optional<Double> max(String fieldName) {
        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorMax(fieldName);
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        viewBuilder.valueFields(Collections.singleton(fieldName)).reduce(operator.getOperatorExpression());
        ViewResponse response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        Map<String, Double> res = (Map<String, Double>) response.rows.get(0).value;
        return Optional.of(res.get("max"));
    }

    @Override
    public Optional<Double> min(String fieldName) {
        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorMin(fieldName);
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        viewBuilder.valueFields(Collections.singleton(fieldName)).reduce(operator.getOperatorExpression());
        ViewResponse response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        Map<String, Double> res = (Map<String, Double>) response.rows.get(0).value;
        return Optional.of(res.get("min"));
    }

    @Override
    public Optional<Double> sum(String fieldName) {
        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorSum(fieldName);
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        viewBuilder.valueFields(Collections.singleton(fieldName)).reduce(operator.getOperatorExpression());
        ViewResponse response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        Map<String, Double> res = (Map<String, Double>) response.rows.get(0).value;
        return Optional.of(res.get("sum"));
    }


    @Override
    public Optional<Double> avg(String fieldName) {
        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorAvg(fieldName);
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        viewBuilder.valueFields(Collections.singleton(fieldName)).reduce(operator.getOperatorExpression());
        ViewResponse response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        Double res = (Double) response.rows.get(0).value;
        return Optional.of(res);
    }

    @Override
    public int count() {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        ViewResponse response = connection.execute(viewBuilder.build());

        if (response.totalRows != null) {
            return response.totalRows;
        } else {
            return response.rows.size();
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators sort(SortOperator sortOperator, SortOperator... sortingOperators) {
        @SuppressWarnings("unchecked") Map<String, String> sortFields = Stream.concat(Stream.of(sortOperator), Stream.of(sortingOperators))
                .flatMap(op -> ((Map<String, String>) op.getOperatorExpression()).entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        viewBuilder.sortFields(sortFields);

        return this;
    }

    @Override
    public CouchDBOperators limit(int limit) {
        viewBuilder.limit(limit);
        return this;
    }

    @Override
    public CouchDBOperators project(String fieldName, String... fieldNames) {
        return this;
    }

    @Override
    public Dataset<Row> toDataframe() {
        return null;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators join(NoSqlDbOperators noSqlDbOperators, JoinOperator jo) {
        return this;
    }
}
