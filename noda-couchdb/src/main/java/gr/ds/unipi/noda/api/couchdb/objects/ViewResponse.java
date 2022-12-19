package gr.ds.unipi.noda.api.couchdb.objects;

import java.util.List;
import java.util.Map;

public class ViewResponse<V> {

    public int total_rows;
    public int offset;
    public List<ViewRow<V>> rows;

    public static class ViewRow<V> {
        public String id;
        public List<Object> key;
        public V value;
        public Map<Object, Object> doc;
    }

    public static class Stats {
        public Double sum;
        public Integer count;
        public Double min;
        public Double max;
        public Double sumsqr;
    }
}