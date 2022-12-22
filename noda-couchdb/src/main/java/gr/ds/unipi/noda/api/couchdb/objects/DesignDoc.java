package gr.ds.unipi.noda.api.couchdb.objects;

import java.util.HashMap;
import java.util.Map;

public class DesignDoc {
    public Map<String, View> views;

    public DesignDoc(View view) {
        views = new HashMap<>();
        views.put(view.getName(), view);
    }
}
