package gr.ds.unipi.noda.api.couchdb.filterOperators.logicalOperators;

import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;

import java.util.Arrays;
import java.util.stream.Stream;

abstract class LogicalOperator extends gr.ds.unipi.noda.api.core.operators.filterOperators.logicalOperators.LogicalOperator<StringBuilder> {
    protected LogicalOperator(FilterOperator<?> filterOperator1, FilterOperator<?> filterOperator2, FilterOperator<?>... filterOperators) {
        super(filterOperator1, filterOperator2, filterOperators);
    }

    abstract protected String operatorSymbol();

    @Override
    public StringBuilder getOperatorExpression() {
        return Arrays
                .stream(getFilterOperatorChildren())
                .flatMap(o -> Stream.of(o.getOperatorExpression(), operatorSymbol()))
                .limit(getFilterOperatorChildren().length * 2L - 1)
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                .insert(0, "(")
                .append(")");
    }

    //        return Collections.singletonMap(
    //                operatorName(),
    //                Arrays.stream(getFilterOperatorChildren())
    //                        .map(FilterOperator::getOperatorExpression)
    //                        .collect(Collectors.toList())
    //        );
}
