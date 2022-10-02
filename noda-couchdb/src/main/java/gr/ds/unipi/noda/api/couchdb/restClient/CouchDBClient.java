package gr.ds.unipi.noda.api.couchdb.restClient;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.*;

public class CouchDBClient {

    public static CouchDBClient newInstance(String host, Integer port, String username, String password) {
        return new CouchDBClient(host, port, username, password);
    }

    public CouchDBCall<FindResult> find(String database, FindRequest findRequest) {
        HttpUrl url = resolveRequestUrl(database, "_find");

        JsonObject jsonObject = new JsonObject();
        jsonObject.add("selector", new Gson().toJsonTree(findRequest.selector()));

        Request request = new Request.Builder().url(url)
                .headers(this.headers)
                .post(RequestBody.create(jsonObject.toString(), JSON))
                .build();

        return new CouchDBCall<>(this.client.newCall(request), FindResult.class);
    }

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private final Headers headers;
    private final HttpUrl serverUrl;
    private final OkHttpClient client;

    private CouchDBClient(String host, Integer port, String username, String password) {
        this.headers = new Headers.Builder().add("accept", "application/json")
                .add("authorization", Credentials.basic(username, password))
                .build();
        this.serverUrl = new HttpUrl.Builder().scheme("http").host(host).port(port).build();
        this.client = new OkHttpClient();
    }

    private HttpUrl resolveRequestUrl(String... paths) {
        HttpUrl.Builder builder = this.serverUrl.newBuilder();
        for (String path : paths) {
            builder.addPathSegment(path);
        }
        return builder.build();
    }

}
