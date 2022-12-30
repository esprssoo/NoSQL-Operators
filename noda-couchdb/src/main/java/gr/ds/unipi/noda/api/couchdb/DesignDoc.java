package gr.ds.unipi.noda.api.couchdb;

import java.util.HashMap;
import java.util.Map;

final class DesignDoc {
    public String _id;
    public String _rev;
    public Map<String, View> views;

    public DesignDoc(View view) {
        views = new HashMap<>();
        views.put(view.getName(), view);
    }
}
