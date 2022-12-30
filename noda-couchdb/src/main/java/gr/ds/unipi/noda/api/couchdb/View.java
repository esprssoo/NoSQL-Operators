package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
        private final String database;
        private final List<String> filters = new ArrayList<>();
        private final Set<String> groupFields = new HashSet<>();
        private final Map<String, String> sortFields = new HashMap<>();
        private final Set<String> valueFields = new HashSet<>();
        private final Map<String, String> reduceExpressions = new HashMap<>();
        private final Map<String, String> rereduceExpressions = new HashMap<>();
        private boolean isReduce = false;
        private boolean isGroup = false;
        private int limit = -1;

        public Builder(String database) {
            this.database = database;
        }

        public View build() {
            ArrayList<String> keys = new ArrayList<>();

            int groupLevel = 0;
            if (!groupFields.isEmpty()) {
                for (String field : groupFields) {
                    keys.add("doc[\"" + field + "\"]");
                }
                groupLevel = groupFields.size();
            }

            boolean descending = false;
            if (!sortFields.isEmpty()) {
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
            map.append("function(doc) {")
                    .append(filters.isEmpty() ? "" : "if (" + String.join("&&", filters) + ")")
                    .append("emit(");
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
            if (reduceExpressions.isEmpty()) {
                reduce.append("function() { return null }");
            } else {
                reduce.append("function(keys, values, rereduce){ if (rereduce) { return {");
                rereduceExpressions.forEach((k, v) -> reduce.append('"')
                        .append(k)
                        .append("\": ")
                        .append(v)
                        .append(','));
                reduce.append("} } else { return {");
                reduceExpressions.forEach((k, v) -> reduce.append('"').append(k).append("\": ").append(v).append(','));
                reduce.append("} } }");
            }

            return new View(database, map.toString(), reduce.toString(), isReduce, isGroup, groupLevel, limit, descending);
        }

        public Builder filter(String filter) {
            this.filters.add(filter);
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
            isReduce = reduce;
            return this;
        }

        public Builder groupField(String groupField) {
            this.groupFields.add(groupField);
            return this;
        }

        public Builder sortFields(Map<String, String> sortFields) {
            this.sortFields.putAll(sortFields);
            return this;
        }

        public Builder valueField(String valueField) {
            this.valueFields.add(valueField);
            return this;
        }

        public Builder reduceExpression(String alias, String reduceExpression, String rereduceExpression) {
            this.reduceExpressions.put(alias, reduceExpression);
            this.rereduceExpressions.put(alias, rereduceExpression);
            return this;
        }
    }

    @SuppressWarnings("unused")
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
