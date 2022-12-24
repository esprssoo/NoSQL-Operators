package gr.ds.unipi.noda.api.couchdb.objects;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class ViewResponse {
    @SerializedName("total_rows")
    public Integer totalRows;
    public Integer offset;
    public List<Row> rows;

    public static class Row {
        public String id;
        public Object key;
        public Object value;
        public Object doc;
    }
}