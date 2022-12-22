package gr.ds.unipi.noda.api.couchdb;

import gr.ds.unipi.noda.api.couchdb.objects.View;

import java.util.Map;
import java.util.Set;

public class CouchDBView {
    private final String database;
    private int limit = -1;
    private boolean group = false;
    private boolean reduce = false;
    private String filter;
    private Map<String, String> sortFields;
    private Set<String> groupFields;

    public CouchDBView(String database) {
        this.database = database;
    }

    public View createViewObject() {
        return new View("new-view", null, null);
    }

    public void setSortFields(Map<String, String> sortFields) {
        this.sortFields = sortFields;
    }

    public boolean isReduce() {
        return reduce;
    }

    public boolean isGroup() {
        return group;
    }

    public String getDatabase() {
        return database;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setGroupFields(Set<String> groupFields) {
        this.groupFields = groupFields;
    }
}
