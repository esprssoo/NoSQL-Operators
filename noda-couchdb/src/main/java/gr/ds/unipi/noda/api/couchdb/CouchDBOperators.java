package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.GsonBuilder;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.operators.aggregateOperators.AggregateOperator;
import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;
import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;
import gr.ds.unipi.noda.api.core.operators.sortOperators.SortOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

final class CouchDBOperators extends NoSqlDbOperators {

    private final CouchDBConnectionManager couchDBConnectionManager = CouchDBConnectionManager.getInstance();
    private final View.Builder viewBuilder;

    private CouchDBOperators(CouchDBConnector connector, String dataCollection, SparkSession sparkSession) {
        super(connector, dataCollection, sparkSession);
        viewBuilder = new View.Builder(dataCollection);
    }

    static CouchDBOperators newCouchDBOperators(CouchDBConnector noSqlDbConnector, String dataCollection, SparkSession sparkSession) {
        return new CouchDBOperators(noSqlDbConnector, dataCollection, sparkSession);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators filter(FilterOperator filterOperator, FilterOperator... filterOperators) {
        Stream.concat(Stream.of(filterOperator), Stream.of(filterOperators))
                .map(op -> (String) op.getOperatorExpression())
                .forEach(viewBuilder::filter);

        return this;
    }

    @Override
    public CouchDBOperators groupBy(String fieldName, String... fieldNames) {
        Stream.concat(Stream.of(fieldName), Stream.of(fieldNames)).forEach(viewBuilder::groupField);
        viewBuilder.group(true);

        return this;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators aggregate(AggregateOperator aggregateOperator, AggregateOperator... aggregateOperators) {
        Stream.concat(Stream.of(aggregateOperator), Stream.of(aggregateOperators)).forEach(op -> {
            String[] expression = (String[]) op.getOperatorExpression();
            assert expression.length == 2;
            viewBuilder.reduceExpression(op.getAlias(), expression[0], expression[1]).valueField(op.getFieldName());
        });

        viewBuilder.reduce(true);

        return this;
    }

    @Override
    public CouchDBOperators distinct(String fieldName) {
        viewBuilder.group(true).reduce(true);
        return groupBy(fieldName);
    }

    @Override
    public void printScreen() {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());
        View.Response response = connection.execute(viewBuilder.build());
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(response));
    }

    @Override
    public Optional<Double> max(String fieldName) {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        AggregateOperator<?> operator = AggregateOperator.aggregateOperator.newOperatorMax(fieldName);

        String[] operatorExpression = (String[]) operator.getOperatorExpression();
        assert operatorExpression.length == 2;

        viewBuilder.reduceExpression(operator.getAlias(), operatorExpression[0], operatorExpression[1])
                .valueField(fieldName)
                .reduce(true)
                .group(false);

        View.Response response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((Double) ((Map<String, ?>) response.rows.get(0).value).get(operator.getAlias()));
    }

    @Override
    public Optional<Double> min(String fieldName) {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        AggregateOperator<?> operator = AggregateOperator.aggregateOperator.newOperatorMin(fieldName);

        String[] operatorExpression = (String[]) operator.getOperatorExpression();
        assert operatorExpression.length == 2;

        viewBuilder.reduceExpression(operator.getAlias(), operatorExpression[0], operatorExpression[1])
                .valueField(fieldName)
                .reduce(true)
                .group(false);

        View.Response response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((Double) response.rows.get(0).value.get(operator.getAlias()));
    }

    @Override
    public Optional<Double> sum(String fieldName) {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        AggregateOperator<?> operator = AggregateOperator.aggregateOperator.newOperatorSum(fieldName);

        String[] operatorExpression = (String[]) operator.getOperatorExpression();
        assert operatorExpression.length == 2;

        viewBuilder.reduceExpression(operator.getAlias(), operatorExpression[0], operatorExpression[1])
                .valueField(fieldName)
                .reduce(true)
                .group(false);

        View.Response response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((Double) response.rows.get(0).value.get(operator.getAlias()));
    }

    @Override
    public Optional<Double> avg(String fieldName) {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        AggregateOperator<?> operator = AggregateOperator.aggregateOperator.newOperatorAvg(fieldName);

        String[] operatorExpression = (String[]) operator.getOperatorExpression();
        assert operatorExpression.length == 2;

        viewBuilder.reduceExpression(operator.getAlias(), operatorExpression[0], operatorExpression[1])
                .valueField(fieldName)
                .reduce(true)
                .group(false);

        View.Response response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((Double) ((Map<String, ?>) response.rows.get(0).value).get(operator.getAlias()));
    }

    @Override
    public int count() {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        View.Response response = connection.execute(viewBuilder.build());

        if (response.totalRows != null) {
            return response.totalRows;
        } else {
            return response.rows.size();
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators sort(SortOperator sortOperator, SortOperator... sortingOperators) {
        Stream.concat(Stream.of(sortOperator), Stream.of(sortingOperators))
                .map(op -> ((Map) op.getOperatorExpression()))
                .forEach(viewBuilder::sortFields);

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
