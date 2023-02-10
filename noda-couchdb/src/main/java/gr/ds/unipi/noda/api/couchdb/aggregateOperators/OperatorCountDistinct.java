package gr.ds.unipi.noda.api.couchdb.aggregateOperators;

public class OperatorCountDistinct extends AggregateOperator {

    private OperatorCountDistinct(String fieldName) {
        super(fieldName, "countDistinct_" + fieldName);
    }

    public static OperatorCountDistinct newOperatorCountDistinct(String fieldName) {
        return new OperatorCountDistinct(fieldName);
    }

    @Override
    protected String reduceStageExpression() {
        return null;
    }

    @Override
    protected String rereduceStageExpression() {
        return null;
    }
}
