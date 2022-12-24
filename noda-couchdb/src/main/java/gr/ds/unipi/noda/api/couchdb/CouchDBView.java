package gr.ds.unipi.noda.api.couchdb;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

final public class CouchDBView {
    transient private final String database;
    transient private final String name;
    transient private final boolean isGroup;
    transient private final int limit;
    transient private final boolean descending;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String map;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String reduce;

    private CouchDBView(String database, String name, String map, String reduce, boolean isGroup, int limit, boolean descending) {
        this.database = database;
        this.name = name;
        this.map = map;
        this.reduce = reduce;
        this.isGroup = isGroup;
        this.limit = limit;
        this.descending = descending;
    }

    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public boolean isGroup() {
        return isGroup;
    }

    public boolean isReduce() {
        return reduce != null;
    }

    public int getLimit() {
        return limit;
    }

    public boolean isDescending() {
        return descending;
    }

    @SuppressWarnings("UnusedReturnValue")
    public static class Builder {
        private String database;
        private String filter;
        private Set<String> groupFields;
        private Map<String, String> sortFields;
        private Set<String> valueFields;
        private boolean isGroup = false;
        private String reduce;
        private int limit = -1;

        public CouchDBView build() {
            ArrayList<String> keys = new ArrayList<>();

            String condition = filter != null ? "if (" + filter + ")" : "";

            boolean descending = false;
            if (sortFields != null) {
                for (Map.Entry<String, String> entry : sortFields.entrySet()) {
                    if (entry.getValue().equals("ascending") && descending) {
                        throw new IllegalStateException("Multiple sorting fields with different sort orders");
                    }

                    descending = entry.getValue().equals("descending");
                    keys.add("doc[\"" + entry.getKey() + "\"]");
                }
            }

            if (groupFields != null) {
                for (String field : groupFields) {
                    keys.add("doc[\"" + field + "\"]");
                }
            }

            String emitKey;
            if (keys.size() == 0) {
                emitKey = "null";
            } else if (keys.size() == 1) {
                emitKey = keys.get(0);
            } else {
                emitKey = keys.toString();
            }

            String emitValue;
            if (valueFields.size() == 0) {
                emitValue = "null";
            } else {
                emitValue = valueFields.stream().map(field -> "doc[\"" + field + "\"]").findFirst().get();
            }

            String map = "function(doc){ " + condition + " emit(" + emitKey + ", " + emitValue + ") }";
            int reduceHash = reduce != null ? reduce.hashCode() : 0;
            String name = Integer.toString(map.hashCode() + reduceHash);

            return new CouchDBView(database, name, map, reduce, isGroup, limit, descending);
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder group(boolean group) {
            isGroup = group;
            return this;
        }

        public Builder reduce(String reduce) {
            this.reduce = reduce;
            return this;
        }

        public Builder groupFields(Set<String> groupFields) {
            this.groupFields = groupFields;
            return this;
        }

        public Builder sortFields(Map<String, String> sortFields) {
            this.sortFields = sortFields;
            return this;
        }

        public Builder valueFields(Set<String> valueFields) {
            this.valueFields = valueFields;
            return this;
        }
    }
}
