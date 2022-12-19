package gr.ds.unipi.noda.api.couchdb.filterOperators.comparisonOperators;

import com.google.gson.Gson;

abstract class ComparisonOperator<U> extends gr.ds.unipi.noda.api.core.operators.filterOperators.comparisonOperators.ComparisonOperator<StringBuilder, U> {
    protected ComparisonOperator(String fieldName, U fieldValue) {
        super(fieldName, fieldValue);
    }

    abstract protected String operatorSymbol();

    @Override
    public StringBuilder getOperatorExpression() {
        String fieldValue = new Gson().toJson(getFieldValue());
        return new StringBuilder()
                .append("doc[\"")
                .append(getFieldName())
                .append("\"]")
                .append(operatorSymbol())
                .append(fieldValue);
    }
}
