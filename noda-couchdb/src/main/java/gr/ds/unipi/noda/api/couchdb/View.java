package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class View {
    transient private final String database;
    transient private final String name;
    transient private final boolean isGroup;
    transient private final boolean isReduce;
    transient private final int groupLevel;
    transient private final int limit;
    transient private final boolean descending;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String map;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String reduce;

private View(String database, String map, String reduce, boolean isReduce, boolean isGroup, int groupLevel, int limit, boolean descending) {
        this.database = database;
        this.name = Integer.toString(map.hashCode() + reduce.hashCode());
        this.map = map;
        this.reduce = reduce;
        this.isReduce = isReduce;
        this.groupLevel = groupLevel;
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

    public int getGroupLevel() {
        return groupLevel;
    }

    public boolean isReduce() {
        return isReduce;
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
        private boolean isReduce;
        private int limit = -1;
        private Map<String, String> reduceExpressions;
        private Map<String, String> rereduceExpressions;

        public View build() {
            ArrayList<String> keys = new ArrayList<>();

            int groupLevel = 0;
            boolean descending = false;

            if (groupFields != null) {
                for (String field : groupFields) {
                    keys.add("doc[\"" + field + "\"]");
                }
                groupLevel = groupFields.size();
            }

            if (sortFields != null) {
                for (Map.Entry<String, String> entry : sortFields.entrySet()) {
                    if (entry.getValue().equals("ascending") && descending) {
                        throw new IllegalStateException("Multiple sorting fields with different sort orders");
                    }

                    descending = entry.getValue().equals("descending");
                    keys.add("doc[\"" + entry.getKey() + "\"]");
                }
            }

            // Create map function
            StringBuilder map = new StringBuilder();
            map.append("function(doc){").append(filter != null ? "if (" + filter + ")" : "").append("emit(");
            if (keys.size() == 0) {
                map.append("null");
            } else if (keys.size() == 1) {
                map.append(keys.get(0));
            } else {
                map.append(keys);
            }
            map.append(",");
            if (valueFields.size() == 0) {
                map.append("null");
            } else {
                map.append("{");
                map.append(valueFields.stream()
                        .map(field -> "\"" + field + "\": doc[\"" + field + "\"]")
                        .collect(Collectors.joining(",")));
                map.append("}");
            }
            map.append(")}");

            // Create reduce function
            StringBuilder reduce = new StringBuilder();
            if (reduceExpressions == null || rereduceExpressions == null) {
                reduce.append("function() { return null }");
            } else {
                reduce.append("function(keys, values, rereduce){ if (rereduce) { return {");
                rereduceExpressions.forEach((k, v) -> reduce.append('"').append(k).append("\": ").append(v).append(','));
                reduce.append("} } else { return {");
                reduceExpressions.forEach((k, v) -> reduce.append('"').append(k).append("\": ").append(v).append(','));
                reduce.append("} } }");
            }

            return new View(database, map.toString(), reduce.toString(), isReduce, isGroup, groupLevel, limit, descending);
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

        public Builder reduce(boolean reduce) {
            this.isReduce = reduce;
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

        public Builder reduceExpressions(Map<String, String> reduceExpressions) {
            this.reduceExpressions = reduceExpressions;
            return this;
        }

        public Builder rereduceExpressions(Map<String, String> rereduceExpressions) {
            this.rereduceExpressions = rereduceExpressions;
            return this;
        }
    }

    public static class Response {
        @SerializedName("total_rows")
        public Integer totalRows;
        public Integer offset;
        public List<Row> rows;

        public static class Row {
            public String id;
            public Object key;
            public Map<String, Object> value;
            public Object doc;
        }
    }
}
