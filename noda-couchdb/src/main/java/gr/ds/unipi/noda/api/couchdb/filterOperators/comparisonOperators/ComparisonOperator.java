package gr.ds.unipi.noda.api.couchdb.filterOperators.comparisonOperators;

import java.util.Collections;
import java.util.Map;

abstract class ComparisonOperator<U> extends gr.ds.unipi.noda.api.core.operators.filterOperators.comparisonOperators.ComparisonOperator<Map<String, Object>, U> {
    protected ComparisonOperator(String fieldName, U fieldValue) {
        super(fieldName, fieldValue);
    }

    abstract protected String operatorName();

    @Override
    public Map<String, Object> getOperatorExpression() {
        return Collections.singletonMap(getFieldName(), Collections.singletonMap(operatorName(), getFieldValue()));
    }
}
