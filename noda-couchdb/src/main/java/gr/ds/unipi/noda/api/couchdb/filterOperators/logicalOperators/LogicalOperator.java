package gr.ds.unipi.noda.api.couchdb.filterOperators.logicalOperators;

import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

abstract class LogicalOperator extends gr.ds.unipi.noda.api.core.operators.filterOperators.logicalOperators.LogicalOperator<Map<String, Object>> {
    protected LogicalOperator(FilterOperator<?> filterOperator1, FilterOperator<?> filterOperator2, FilterOperator<?>... filterOperators) {
        super(filterOperator1, filterOperator2, filterOperators);
    }

    abstract protected String operatorName();

    @Override
    public Map<String, Object> getOperatorExpression() {
        return Collections.singletonMap(operatorName(), Arrays.stream(getFilterOperatorChildren())
                .map(FilterOperator::getOperatorExpression)
                .collect(Collectors.toList()));
    }
}
