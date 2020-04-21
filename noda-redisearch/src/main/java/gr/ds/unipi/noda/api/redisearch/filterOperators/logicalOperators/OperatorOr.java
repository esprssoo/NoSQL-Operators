package gr.ds.unipi.noda.api.redisearch.filterOperators.logicalOperators;

import gr.ds.unipi.noda.api.core.constants.StringPool;
import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;
import io.redisearch.querybuilder.Node;
import io.redisearch.querybuilder.QueryBuilder;
import io.redisearch.querybuilder.QueryNode;

final class OperatorOr extends LogicalOperator {

    private OperatorOr(FilterOperator filterOperator1, FilterOperator filterOperator2, FilterOperator... filterOperators) {
        super(filterOperator1, filterOperator2, filterOperators);
    }

    static OperatorOr newOperatorOr(FilterOperator filterOperator1, FilterOperator filterOperator2, FilterOperator... filterOperators) {
        return new OperatorOr(filterOperator1, filterOperator2, filterOperators);
    }

    public Node getOperatorExpression() {
        QueryNode queryNode = QueryBuilder.union();
        for (FilterOperator f : getFilterOperatorChildren())
            queryNode.add((Node) f.getOperatorExpression());
        return queryNode;
    }

    @Override
    protected String getPostOperatorField() {
        return StringPool.DOUBLE_PIPE;
    }

}
