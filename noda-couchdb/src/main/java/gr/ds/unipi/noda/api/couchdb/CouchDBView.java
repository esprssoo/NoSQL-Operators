package gr.ds.unipi.noda.api.couchdb;

public class CouchDBView {
    private int limit = -1;
    private boolean group = false;
    private boolean reduce = false;

    public CouchDBView() {
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setGroup(boolean group) {
        this.group = group;
    }

    public void setReduce(boolean reduce) {
        this.reduce = reduce;
    }
}
