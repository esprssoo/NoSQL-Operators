package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
        private List<String> filters = new ArrayList<>();
        private Set<String> groupFields = new HashSet<>();
        private Map<String, String> sortFields = new HashMap<>();
        private Set<String> valueFields = new HashSet<>();
        private Set<String> projectFields = new HashSet<>();
        private Map<String, String[]> reduceExpressions = new HashMap<>();
        private boolean isReduce = false;
        private boolean isGroup = false;
        private int limit = -1;

        public Builder(String database) {
            this.database = database;
        }

        public Builder(Builder self) {
            this.database = self.database;
            this.filters = new ArrayList<>(self.filters);
            this.groupFields = new HashSet<>(self.groupFields);
            this.sortFields = new HashMap<>(self.sortFields);
            this.valueFields = new HashSet<>(self.valueFields);
            this.projectFields = new HashSet<>(self.projectFields);
            this.reduceExpressions = new HashMap<>(self.reduceExpressions);
            this.isGroup = self.isGroup;
            this.isReduce = self.isReduce;
            this.limit = self.limit;
        }

        public View build() {
            ArrayList<String> keys = new ArrayList<>();
            boolean sortDescending = false;

            if (isGroup) {
                groupFields.forEach(field -> keys.add("doc[\"" + field + "\"]"));
            } else {
                // NOTE: Sort does not work together with Group By
                for (Map.Entry<String, String> entry : sortFields.entrySet()) {
                    if (entry.getValue().equals("ascending") && sortDescending) {
                        throw new IllegalStateException("Multiple sorting fields with different sort orders");
                    }

                    sortDescending = entry.getValue().equals("descending");
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
                map.append("[").append(String.join(",", keys)).append("]");
            }
            map.append(",");
            if (valueFields.size() == 0 && projectFields.size() == 0) {
                map.append("null");
            } else {
                map.append("{");
                Stream.concat(valueFields.stream(), projectFields.stream())
                        .forEach(field -> map.append("\"")
                                .append(field)
                                .append("\": doc[\"")
                                .append(field)
                                .append("\"],"));
                map.append("}");
            }
            map.append(")}");

            // Create reduce function
            StringBuilder reduce = new StringBuilder();
            reduce.append("function(keys, values, rereduce){ if (rereduce) { return {");
            projectFields.forEach(field -> reduce.append('"')
                    .append(field)
                    .append("\": values.flatMap(a => a[\"")
                    .append(field)
                    .append("\"]),"));
            reduceExpressions.forEach((k, v) -> reduce.append('"').append(k).append("\": ").append(v[1]).append(','));
            reduce.append("} } else { return {");
            projectFields.forEach(field -> reduce.append('"')
                    .append(field)
                    .append("\": values.map(a => a[\"")
                    .append(field)
                    .append("\"]),"));
            reduceExpressions.forEach((k, v) -> reduce.append('"').append(k).append("\": ").append(v[0]).append(','));
            reduce.append("} } }");

            return new View(database, map.toString(), reduce.toString(), isReduce, isGroup, groupFields.size(), limit, sortDescending);
        }

        public Builder filter(String filter) {
            this.filters.add(filter);
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder group(boolean isGroup) {
            this.isGroup = isGroup;
            return this;
        }

        public Builder reduce(boolean isReduce) {
            this.isReduce = isReduce;
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

        public Builder projectField(String projectField) {
            this.projectFields.add(projectField);
            return this;
        }

        public Builder reduceExpressions(String alias, String[] reduceExpressions) {
            assert reduceExpressions.length == 2;
            this.reduceExpressions.put(alias, reduceExpressions);
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
