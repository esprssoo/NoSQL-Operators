package gr.ds.unipi.noda.api.couchdb.filterOperators.comparisonOperators;

import gr.ds.unipi.noda.api.core.operators.filterOperators.comparisonOperators.BaseComparisonOperatorFactory;
import gr.ds.unipi.noda.api.core.operators.filterOperators.comparisonOperators.ComparisonOperator;

import java.util.Date;

public final class CouchDBComparisonOperatorFactory extends BaseComparisonOperatorFactory {
    @Override
    public ComparisonOperator<StringBuilder, Double> newOperatorGte(String fieldName, double fieldValue) {
        return OperatorGreaterThanEqual.newOperatorGreaterThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Integer> newOperatorGte(String fieldName, int fieldValue) {
        return OperatorGreaterThanEqual.newOperatorGreaterThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Float> newOperatorGte(String fieldName, float fieldValue) {
        return OperatorGreaterThanEqual.newOperatorGreaterThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Short> newOperatorGte(String fieldName, short fieldValue) {
        return OperatorGreaterThanEqual.newOperatorGreaterThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Long> newOperatorGte(String fieldName, long fieldValue) {
        return OperatorGreaterThanEqual.newOperatorGreaterThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Date> newOperatorGte(String fieldName, Date fieldValue) {
        return OperatorGreaterThanEqual.newOperatorGreaterThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Double> newOperatorGt(String fieldName, double fieldValue) {
        return OperatorGreaterThan.newOperatorGreaterThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Integer> newOperatorGt(String fieldName, int fieldValue) {
        return OperatorGreaterThan.newOperatorGreaterThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Float> newOperatorGt(String fieldName, float fieldValue) {
        return OperatorGreaterThan.newOperatorGreaterThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Short> newOperatorGt(String fieldName, short fieldValue) {
        return OperatorGreaterThan.newOperatorGreaterThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Long> newOperatorGt(String fieldName, long fieldValue) {
        return OperatorGreaterThan.newOperatorGreaterThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Date> newOperatorGt(String fieldName, Date fieldValue) {
        return OperatorGreaterThan.newOperatorGreaterThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Double> newOperatorLte(String fieldName, double fieldValue) {
        return OperatorLessThanEqual.newOperatorLessThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Integer> newOperatorLte(String fieldName, int fieldValue) {
        return OperatorLessThanEqual.newOperatorLessThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Float> newOperatorLte(String fieldName, float fieldValue) {
        return OperatorLessThanEqual.newOperatorLessThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Short> newOperatorLte(String fieldName, short fieldValue) {
        return OperatorLessThanEqual.newOperatorLessThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Long> newOperatorLte(String fieldName, long fieldValue) {
        return OperatorLessThanEqual.newOperatorLessThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Date> newOperatorLte(String fieldName, Date fieldValue) {
        return OperatorLessThanEqual.newOperatorLessThanEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Double> newOperatorLt(String fieldName, double fieldValue) {
        return OperatorLessThan.newOperatorLessThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Integer> newOperatorLt(String fieldName, int fieldValue) {
        return OperatorLessThan.newOperatorLessThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Float> newOperatorLt(String fieldName, float fieldValue) {
        return OperatorLessThan.newOperatorLessThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Short> newOperatorLt(String fieldName, short fieldValue) {
        return OperatorLessThan.newOperatorLessThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Long> newOperatorLt(String fieldName, long fieldValue) {
        return OperatorLessThan.newOperatorLessThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Date> newOperatorLt(String fieldName, Date fieldValue) {
        return OperatorLessThan.newOperatorLessThan(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Double> newOperatorEq(String fieldName, double fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Boolean> newOperatorEq(String fieldName, boolean fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, String> newOperatorEq(String fieldName, String fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Integer> newOperatorEq(String fieldName, int fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Float> newOperatorEq(String fieldName, float fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Short> newOperatorEq(String fieldName, short fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Long> newOperatorEq(String fieldName, long fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Date> newOperatorEq(String fieldName, Date fieldValue) {
        return OperatorEqual.newOperatorEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Double> newOperatorNe(String fieldName, double fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Boolean> newOperatorNe(String fieldName, boolean fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, String> newOperatorNe(String fieldName, String fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Integer> newOperatorNe(String fieldName, int fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Float> newOperatorNe(String fieldName, float fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Short> newOperatorNe(String fieldName, short fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Long> newOperatorNe(String fieldName, long fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }

    @Override
    public ComparisonOperator<StringBuilder, Date> newOperatorNe(String fieldName, Date fieldValue) {
        return OperatorNotEqual.newOperatorNotEqual(fieldName, fieldValue);
    }
}
