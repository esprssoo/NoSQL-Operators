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
        viewBuilder.group(true).reduce(true);
        return groupBy(fieldName);
    }

    @Override
    public void printScreen() {
        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());
        ViewResponse response = connection.execute(viewBuilder);
        System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(response));
    }

    @Override
    public Optional<Double> max(String fieldName) {
//        CouchDBConnector.CouchDBConnection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

//        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorMax(fieldName);
//
//        String mapFunction = buildMapFunction(fieldName);
//        String reduceFunction = operator.getOperatorExpression();
//        String db = getDataCollection();
//        String viewName = Integer.toString(Objects.hash(mapFunction, reduceFunction));
//
//        ensureViewExists(db, viewName, mapFunction, reduceFunction);
//
//        Type type = new TypeToken<ViewResponse<ViewResponse.Stats>>() {
//        }.getType();
//
//        try {
//            ViewResponse<ViewResponse.Stats> response = connector.view(db, viewName, true, limit, type);
//            return Optional.of(response.rows.get(0).value.max);
//        } catch (IOException e) {
//            // TODO: error
//            e.printStackTrace();
//            return Optional.empty();
//        }
        return Optional.empty();
    }

    @Override
    public Optional<Double> min(String fieldName) {
//        CouchDBConnector connector = couchDBConnectionManager.getConnection(getNoSqlDbConnector());
//        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorMin(fieldName);
//
//        String mapFunction = buildMapFunction(fieldName);
//        String reduceFunction = operator.getOperatorExpression();
//        String db = getDataCollection();
//        String viewName = Integer.toString(Objects.hash(mapFunction, reduceFunction));
//
//        ensureViewExists(db, viewName, mapFunction, reduceFunction);
//
//        Type type = new TypeToken<ViewResponse<ViewResponse.Stats>>() {
//        }.getType();
//
//        try {
//            ViewResponse<ViewResponse.Stats> response = connector.view(db, viewName, true, limit, type);
//            return Optional.of(response.rows.get(0).value.min);
//        } catch (IOException e) {
//            // TODO: error
//            e.printStackTrace();
//            return Optional.empty();
//        }
        return Optional.empty();
    }

    @Override
    public Optional<Double> sum(String fieldName) {
//        CouchDBConnector connector = couchDBConnectionManager.getConnection(getNoSqlDbConnector());
//        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorSum(fieldName);
//
//        String mapFunction = buildMapFunction(fieldName);
//        String reduceFunction = operator.getOperatorExpression();
//        String db = getDataCollection();
//        String viewName = Integer.toString(Objects.hash(mapFunction, reduceFunction));
//
//        ensureViewExists(db, viewName, mapFunction, reduceFunction);
//
//        Type type = new TypeToken<ViewResponse<ViewResponse.Stats>>() {
//        }.getType();
//
//        try {
//            ViewResponse<ViewResponse.Stats> response = connector.view(db, viewName, true, limit, type);
//            return Optional.of(response.rows.get(0).value.sum);
//        } catch (IOException e) {
//            // TODO: error
//            e.printStackTrace();
//            return Optional.empty();
//        }
        return Optional.empty();
    }


    @Override
    public Optional<Double> avg(String fieldName) {
//        CouchDBConnector connector = couchDBConnectionManager.getConnection(getNoSqlDbConnector());
//        AggregateOperator<String> operator = AggregateOperator.aggregateOperator.newOperatorAvg(fieldName);
//
//        String mapFunction = buildMapFunction(fieldName);
//        String reduceFunction = operator.getOperatorExpression();
//        String db = getDataCollection();
//        String viewName = Integer.toString(Objects.hash(mapFunction, reduceFunction));
//
//        ensureViewExists(db, viewName, mapFunction, reduceFunction);
//
//        Type type = new TypeToken<ViewResponse<Double>>() {
//        }.getType();
//
//        try {
//            ViewResponse<Double> response = connector.view(db, viewName, true, limit, type);
//            return Optional.of(response.rows.get(0).value);
//        } catch (IOException e) {
//            // TODO: error
//            e.printStackTrace();
//            return Optional.empty();
//        }
        return Optional.empty();
    }

    @Override
    public int count() {
//        CouchDBConnector connector = couchDBConnectionManager.getConnection(getNoSqlDbConnector());
//        boolean shouldUseReduceFunction = groupFields.size() > 0;
//
//        String mapFunction = buildMapFunction(null);
//        String reduceFunction = shouldUseReduceFunction ? "_count" : null;
//        String db = getDataCollection();
//        String viewName = Integer.toString(Objects.hash(mapFunction, reduceFunction));
//
//        ensureViewExists(db, viewName, mapFunction, reduceFunction);
//
//        Type type = new TypeToken<ViewResponse<Integer>>() {
//        }.getType();
//
//        try {
//            ViewResponse<Integer> response = connector.view(db, viewName, shouldUseReduceFunction, limit, type);
//
//            if (shouldUseReduceFunction) {
//                // TODO: επιστρέφω την πρώτη τιμή που μου επιστρέφει ένα groupBy
//                return response.rows.get(0).value;
//            } else {
//                return response.total_rows;
//            }
//        } catch (IOException e) {
//            // TODO: error
//            throw new RuntimeException(e);
//        }
        return 0;
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

//    private String buildMapFunction(String valueField) {
//        String finalValueField = valueField == null ? "null" : "doc[\"" + valueField + "\"]";
//
//        String keys = groupFields.size() > 0
//                      ? groupFields.stream().map(field -> new StringBuilder("doc[\"")
//                .append(valueField)
//                .append("\"]")).collect(Collectors.toList()).toString()
//                      : "null";
//
//        String values = Stream
//                .of("doc['weight']", "doc['weight']")
//                .collect(Collectors.toList())
//                .toString();
//
//        String emitting = "emit(" + keys + ", " + values + ");";
//
//        return "function(doc) { if (" + filterCondition + ") {" + emitting + "}}";
//    }
}
