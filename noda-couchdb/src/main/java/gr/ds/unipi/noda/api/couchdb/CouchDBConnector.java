package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.Gson;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnector;
import gr.ds.unipi.noda.api.couchdb.objects.DesignDoc;
import gr.ds.unipi.noda.api.couchdb.objects.ViewResponse;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CouchDBConnector implements NoSqlDbConnector<CouchDBConnector.CouchDBConnection> {
    private final static MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private final static Gson GSON = new Gson();
    private static OkHttpClient CLIENT;
    private final HttpUrl serverUrl;
    private final String credentials;

    private CouchDBConnector(HttpUrl serverUrl, String credentials) {
        this.serverUrl = serverUrl;
        this.credentials = credentials;
    }

    public static CouchDBConnector newCouchDBConnector(List<Map.Entry<String, Integer>> addresses, String username, String password) {
        String host = addresses.get(0).getKey();
        int port = addresses.get(0).getValue();

        HttpUrl serverUrl = new HttpUrl.Builder().scheme("http").host(host).port(port).build();
        String credentials = Credentials.basic(username, password);

        return new CouchDBConnector(serverUrl, credentials);
    }

    @Override
    public CouchDBConnection createConnection() {
        CLIENT = new OkHttpClient.Builder().authenticator((route, response) -> response.request()
                .newBuilder()
                .header("authorization", credentials)
                .build()).build();
        return new CouchDBConnection(serverUrl);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    static class CouchDBConnection {
        private final HttpUrl serverUrl;
        private final Headers headers;

        CouchDBConnection(HttpUrl serverUrl) {
            this.serverUrl = serverUrl;
            this.headers = new Headers.Builder().add("accept", "application/json").build();
        }

        public ViewResponse execute(CouchDBView view) {
            try {
                // Create or update the internal design document if it doesn't exist
                updateDesignDoc(view);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }

            HttpUrl url = resolveUrl(view.getDatabase(), "_design", "noda", "_view", view.getName());

            Map<String, Object> body = new HashMap<>();
            body.put("reduce", view.isReduce());
            body.put("include_docs", !view.isReduce());
            body.put("group", view.isGroup());
            body.put("descending", view.isDescending());
            int limit = view.getLimit();
            if (limit >= 0) {
                body.put("limit", view.getLimit());
            }

            Request request = new Request.Builder().url(url)
                    .headers(headers)
                    .post(RequestBody.create(GSON.toJson(body), JSON))
                    .build();

            try (Response res = CLIENT.newCall(request).execute()) {
                assert res.body() != null;

                if (res.code() != 200) {
                    System.err.println(res.body().string());
                    return null;
                }

                return GSON.fromJson(res.body().charStream(), ViewResponse.class);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private DesignDoc getDesignDoc(String db) throws IOException {
            HttpUrl url = resolveUrl(db, "_design", "noda");

            Request request = new Request.Builder().url(url).headers(headers).get().build();

            try (Response res = CLIENT.newCall(request).execute()) {
                if (res.code() != 200) {
                    return null;
                }

                assert res.body() != null;
                return GSON.fromJson(res.body().charStream(), DesignDoc.class);
            }
        }

        private void updateDesignDoc(CouchDBView view) throws IOException {
            HttpUrl url = resolveUrl(view.getDatabase(), "_design", "noda");

            DesignDoc designDoc = getDesignDoc(view.getDatabase());
            if (designDoc == null) {
                designDoc = new DesignDoc(view);
            } else if (!designDoc.views.containsKey(view.getName())) {
                designDoc.views.put(view.getName(), view);
            } else {
                return;
            }

            Request request = new Request.Builder().url(url)
                    .headers(headers)
                    .put(RequestBody.create(GSON.toJson(designDoc), JSON))
                    .build();

            try (Response res = CLIENT.newCall(request).execute()) {
                if (res.code() != 201) {
                    assert res.body() != null;
                    throw new RuntimeException("Failed to create design document. " + res.body().string());
                }
            }
        }

        private HttpUrl resolveUrl(String... paths) {
            HttpUrl.Builder url = serverUrl.newBuilder();
            for (String path : paths) {
                url.addPathSegment(path);
            }
            return url.build();
        }
    }
}
