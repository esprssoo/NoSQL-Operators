package gr.ds.unipi.noda.api.couchdb.restClient;

import java.util.List;
import java.util.Map;

public class FindResult {

    private List<Map<String, Object>> docs;
    private String warning;
    private String bookmark;

    protected FindResult() {
    }

    public List<Map<String, Object>> docs() {
        return this.docs;
    }

    public String warning() {
        return this.warning;
    }

    public String bookmark() {
        return this.bookmark;
    }

}
