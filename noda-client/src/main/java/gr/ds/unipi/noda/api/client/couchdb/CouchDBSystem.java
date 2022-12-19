package gr.ds.unipi.noda.api.client.couchdb;

import gr.ds.unipi.noda.api.client.NoSqlDbSystem;
import gr.ds.unipi.noda.api.couchdb.CouchDBConnectionFactory;
import gr.ds.unipi.noda.api.couchdb.CouchDBConnector;

public class CouchDBSystem extends NoSqlDbSystem {

    private final CouchDBConnector connector;

    private CouchDBSystem(Builder builder) {
        super(builder, new CouchDBConnectionFactory());
        connector = CouchDBConnector.newCouchDBConnector(getAddresses(),
                builder.username,
                builder.password
        );
    }

    @Override
    protected CouchDBConnector getConnector() {
        return connector;
    }

    @Override
    public int getDefaultPort() {
        return 5984;
    }

    public static class Builder extends NoSqlDbSystem.Builder<Builder> {
        private final String username;
        private final String password;

        public Builder(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public NoSqlDbSystem build() {
            return new CouchDBSystem(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
