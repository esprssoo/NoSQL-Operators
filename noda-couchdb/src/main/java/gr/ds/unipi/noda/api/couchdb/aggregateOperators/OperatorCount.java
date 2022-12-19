package gr.ds.unipi.noda.api.couchdb.aggregateOperators;

final class OperatorCount extends AggregateOperator {

    private OperatorCount(String fieldName) {
        super(fieldName, "count");
    }

    public static OperatorCount newOperatorCount() {
        return new OperatorCount("");
    }

    @Override
    protected String reduceFunction() {
        // TODO: _stats function REQUIRES numeric values only
        return null;
    }
}
