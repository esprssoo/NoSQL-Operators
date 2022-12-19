package gr.ds.unipi.noda.api.couchdb.aggregateOperators;

abstract class AggregateOperator extends gr.ds.unipi.noda.api.core.operators.aggregateOperators.AggregateOperator<String> {

    protected AggregateOperator(String fieldName, String alias) {
        super(fieldName, alias);
    }

    abstract protected String reduceFunction();

    @Override
    public String getOperatorExpression() {
        return reduceFunction();
    }
}
