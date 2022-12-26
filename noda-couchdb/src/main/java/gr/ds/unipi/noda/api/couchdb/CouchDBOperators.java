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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class CouchDBOperators extends NoSqlDbOperators {

    private final CouchDBConnectionManager couchDBConnectionManager = CouchDBConnectionManager.getInstance();
    private final View.Builder viewBuilder;

    private CouchDBOperators(CouchDBConnector connector, String dataCollection, SparkSession sparkSession) {
        super(connector, dataCollection, sparkSession);
        viewBuilder = new View.Builder().database(dataCollection);
    }

    private CouchDBOperators(CouchDBOperators self, View.Builder viewBuilder) {
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

        return new CouchDBOperators(this, new View.Builder().database(getDataCollection()).filter(filter));
    }

    @Override
    public CouchDBOperators groupBy(String fieldName, String... fieldNames) {
        viewBuilder.group(true)
                .groupFields(Stream.concat(Stream.of(fieldName), Stream.of(fieldNames)).collect(Collectors.toSet()));
        return this;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public CouchDBOperators aggregate(AggregateOperator aggregateOperator, AggregateOperator... aggregateOperators) {
        String[] operatorExpressions = (String[]) aggregateOperator.getOperatorExpression();
        HashMap<String, String> reduceExpressions = new HashMap<>();
        HashMap<String, String> rereduceExpressions = new HashMap<>();
        Set<String> valueFields = new HashSet<>();

        valueFields.add(aggregateOperator.getFieldName());
        reduceExpressions.put(aggregateOperator.getAlias(), operatorExpressions[0]);
        rereduceExpressions.put(aggregateOperator.getAlias(), operatorExpressions[1]);

        for (AggregateOperator<?> operator : aggregateOperators) {
            operatorExpressions = (String[]) operator.getOperatorExpression();

            valueFields.add(operator.getFieldName());
            reduceExpressions.put(operator.getAlias(), operatorExpressions[0]);
            rereduceExpressions.put(operator.getAlias(), operatorExpressions[1]);
        }

        viewBuilder.valueFields(valueFields)
                .reduceExpressions(reduceExpressions)
                .rereduceExpressions(rereduceExpressions)
                .reduce(true);

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

        viewBuilder.reduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[0]))
                .rereduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[1]))
                .valueFields(Collections.singleton(fieldName))
                .reduce(true)
                .group(true);

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

        viewBuilder.reduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[0]))
                .rereduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[1]))
                .valueFields(Collections.singleton(fieldName))
                .reduce(true)
                .group(true);

        View.Response response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((Double) ((Map<String, ?>) response.rows.get(0).value).get(operator.getAlias()));
    }

    @Override
    public Optional<Double> sum(String fieldName) {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        AggregateOperator<?> operator = AggregateOperator.aggregateOperator.newOperatorSum(fieldName);
        String[] operatorExpression = (String[]) operator.getOperatorExpression();

        viewBuilder.reduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[0]))
                .rereduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[1]))
                .valueFields(Collections.singleton(fieldName))
                .reduce(true)
                .group(true);

        View.Response response = connection.execute(viewBuilder.build());

        if (response.rows.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((Double) ((Map<String, ?>) response.rows.get(0).value).get(operator.getAlias()));
    }


    @Override
    public Optional<Double> avg(String fieldName) {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        AggregateOperator<?> operator = AggregateOperator.aggregateOperator.newOperatorAvg(fieldName);
        String[] operatorExpression = (String[]) operator.getOperatorExpression();

        viewBuilder.reduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[0]))
                .rereduceExpressions(Collections.singletonMap(operator.getAlias(), operatorExpression[1]))
                .valueFields(Collections.singleton(fieldName))
                .reduce(true)
                .group(true);

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
