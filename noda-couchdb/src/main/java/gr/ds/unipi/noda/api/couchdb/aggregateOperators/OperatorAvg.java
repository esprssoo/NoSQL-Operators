package gr.ds.unipi.noda.api.couchdb.aggregateOperators;

final class OperatorAvg extends AggregateOperator {

    private OperatorAvg(String fieldName) {
        super(fieldName, "avg_" + fieldName);
    }

    public static OperatorAvg newOperatorAvg(String fieldName) {
        return new OperatorAvg(fieldName);
    }

    @Override
    protected String reduceFunction() {
        return "function(keys, values) {"
               + "var sum = 0;"
               + "for (var value of values) sum += value;"
               + "return sum / values.length"
               + "}";
    }
}
