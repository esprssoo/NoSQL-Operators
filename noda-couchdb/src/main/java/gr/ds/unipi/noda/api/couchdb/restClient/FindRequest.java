package gr.ds.unipi.noda.api.couchdb.restClient;

public class FindRequest {

    private final Object selector;

    public FindRequest(Object selector) {
        this.selector = selector;
    }

    public Object selector() {
        return this.selector;
    }

}
