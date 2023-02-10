package gr.ds.unipi.noda.api.couchdb.aggregateOperators;

final class OperatorSum extends AggregateOperator {

    private OperatorSum(String fieldName) {
        super(fieldName, "sum_" + fieldName);
    }

    public static OperatorSum newOperatorSum(String fieldName) {
        return new OperatorSum(fieldName);
    }

    @Override
    protected String reduceFunction() {
        return "_stats";
    }
}
