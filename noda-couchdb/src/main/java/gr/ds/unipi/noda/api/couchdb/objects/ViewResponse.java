package gr.ds.unipi.noda.api.couchdb.objects;

import java.util.List;

public class ViewResponse {
    public int total_rows;
    public int offset;
    public List<Row> rows;

    static class Row {
        public String id;
        public Object key;
        public Object value;
        public Object doc;
    }
}