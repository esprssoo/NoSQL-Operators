package gr.ds.unipi.noda.api.couchdb.objects;

import gr.ds.unipi.noda.api.couchdb.CouchDBView;

import java.util.HashMap;
import java.util.Map;

public class DesignDoc {
    public String _id;
    public String _rev;
    public Map<String, CouchDBView> views;

    public DesignDoc(CouchDBView view) {
        views = new HashMap<>();
        views.put(view.getName(), view);
    }
}
