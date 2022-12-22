package gr.ds.unipi.noda.api.couchdb.objects;

public class View {
    private transient final String name;
    private final String map;
    private final String reduce;

    public View(String name, String map, String reduce) {
        this.name = name;
        this.map = map;
        this.reduce = reduce;
    }

    public String getName() {
        return name;
    }

    public String getMap() {
        return map;
    }

    public String getReduce() {
        return reduce;
    }
}