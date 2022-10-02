package gr.ds.unipi.noda.api.couchdb;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnectionManager;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnector;
import gr.ds.unipi.noda.api.couchdb.restClient.CouchDBClient;

final class CouchDBConnectionManager extends NoSqlDbConnectionManager<CouchDBClient> {

    private static final CouchDBConnectionManager INSTANCE = new CouchDBConnectionManager();

    private CouchDBConnectionManager() {

    }

    public static CouchDBConnectionManager getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean closeConnection(NoSqlDbConnector noSqlDbConnector) {
        return false;
    }

    @Override
    public boolean closeConnections() {
        return false;
    }
}
