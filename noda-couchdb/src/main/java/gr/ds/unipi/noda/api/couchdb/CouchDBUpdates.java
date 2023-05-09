package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnector;
import gr.ds.unipi.noda.api.core.nosqldb.modifications.FieldValue;
import gr.ds.unipi.noda.api.core.nosqldb.modifications.NoSqlDbUpdates;
import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

final class CouchDBUpdates extends NoSqlDbUpdates {
    private final CouchDBConnectionManager couchDBConnectionManager = CouchDBConnectionManager.getInstance();
    private final List<Update> updates;

    private CouchDBUpdates(NoSqlDbConnector noSqlDbConnector, String dataCollection) {
        super(noSqlDbConnector, dataCollection);
        updates = Collections.emptyList();
    }

    private CouchDBUpdates(CouchDBUpdates self, List<Update> updates) {
        super(self.getNoSqlDbConnector(), self.getDataCollection());
        this.updates = updates;
    }

    public static CouchDBUpdates newCouchDbUpdates(CouchDBConnector connector, String s) {
        return new CouchDBUpdates(connector, s);
    }

    @Override
    public NoSqlDbUpdates flush() {
        if (updates.isEmpty()) {
            return this;
        }

        CouchDBConnector.Connection connection = couchDBConnectionManager.getConnection(getNoSqlDbConnector());

        for (Update update : updates) {
            try {
                ViewResponse res = connection.runQuery(getDataCollection(), update.viewQuery);

                if (res == null) {
                    continue;
                }

                List<JsonObject> docs = res.rows.stream().map(row -> {
                    for (FieldValue<?> fv : update.fieldValues) {
                        row.doc.add(fv.getField(), new Gson().toJsonTree(fv.getValue()));
                    }

                    return row.doc;
                }).collect(Collectors.toList());

                connection.bulkDocs(getDataCollection(), docs);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return this;
    }

    @Override
    public NoSqlDbUpdates update(FilterOperator filterOperator, FieldValue fv, FieldValue... fvs) {
        List<FieldValue> fieldValues = new ArrayList<>();
        fieldValues.add(fv);
        Collections.addAll(fieldValues, fvs);

        ViewQuery viewQuery = new ViewQuery();
        viewQuery.addFilter(filterOperator);

        List<Update> updates = new ArrayList<>(this.updates);
        updates.add(new Update(viewQuery, fieldValues));

        return new CouchDBUpdates(this, updates);
    }

    private static class Update {
        private final ViewQuery viewQuery;
        private final List<FieldValue> fieldValues;

        private Update(ViewQuery viewQuery, List<FieldValue> fieldValues) {
            this.viewQuery = viewQuery;
            this.fieldValues = fieldValues;
        }
    }
}
