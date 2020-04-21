package gr.ds.unipi.noda.api.core.operators.filterOperators.geographicalOperators;

import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;
import gr.ds.unipi.noda.api.core.operators.filterOperators.geographicalOperators.geometries.Geometry;

public abstract class GeographicalOperator<T, U extends Geometry> implements FilterOperator<T> {

    private final String fieldName;
    private final U geometry;

    protected GeographicalOperator(String fieldName, U geometry) {
        this.fieldName = fieldName;
        this.geometry = geometry;
    }

    protected String getFieldName() {
        return fieldName;
    }

    public U getGeometry() {
        return geometry;
    }

}
