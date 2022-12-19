package gr.ds.unipi.noda.api.couchdb;

import com.google.gson.Gson;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnector;
import gr.ds.unipi.noda.api.couchdb.objects.ViewResponse;
import org.apache.hc.client5.http.ContextBuilder;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class CouchDBConnector implements NoSqlDbConnector<CouchDBConnector> {
    private static final Gson GSON = new Gson();
    private final CloseableHttpClient httpClient;
    private final HttpHost httpHost;
    private final HttpClientContext httpContext;

    private CouchDBConnector(CloseableHttpClient httpClient, HttpHost httpHost, HttpClientContext context) {
        this.httpClient = httpClient;
        this.httpHost = httpHost;
        this.httpContext = context;
    }

    public static CouchDBConnector newCouchDBConnector(List<Map.Entry<String, Integer>> addresses, String username, String password) {
        String host = addresses.get(0).getKey();
        int port = addresses.get(0).getValue();

        HttpHost httpHost = new HttpHost(host, port);

        HttpClientContext context = ContextBuilder
                .create()
                .preemptiveBasicAuth(httpHost,
                        new UsernamePasswordCredentials(username, password.toCharArray())
                )
                .build();

        CloseableHttpClient httpClient = HttpClients
                .custom()
                .setDefaultHeaders(Collections.singletonList(new BasicHeader(HttpHeaders.CONTENT_TYPE,
                        "application/json"
                )))
                .build();

        return new CouchDBConnector(httpClient, httpHost, context);
    }

    public <T> ViewResponse<T> view(String database, String viewName, boolean reduce, int limit, Type type)
    throws IOException {
        HttpPost post = new HttpPost(resolveUri(database, "_design/noda/_view", viewName));

        Map<String, Object> body = new HashMap<>();
        body.put("reduce", reduce);
        body.put("group", reduce);
        body.put("include_docs", !reduce);
        if (limit >= 0) {
            body.put("limit", limit);
        }

        post.setEntity(new StringEntity(GSON.toJson(body)));
        return httpClient.execute(post, httpContext, res -> {
            String str = EntityUtils.toString(res.getEntity());
            // System.out.println(str);
            return GSON.fromJson(str, type);
        });
    }

    public Optional<DesignDoc> getInternalDesignDoc(String database) throws IOException {
        URI uri = resolveUri(database, "_design", "noda");
        return httpClient.execute(new HttpGet(uri), httpContext, res -> {
            if (res.getCode() == 404) {
                return Optional.empty();
            }

            return Optional.of(new Gson().fromJson(EntityUtils.toString(res.getEntity()),
                    DesignDoc.class
            ));
        });
    }

    public void putInternalDesignDoc(String database, DesignDoc designDoc) throws Exception {
        URI uri = resolveUri(database, "_design", "noda");
        HttpPut req = new HttpPut(uri);

        req.setEntity(new StringEntity(new Gson().toJson(designDoc)));

        httpClient.execute(req, httpContext, res -> {
            String body = new Gson().toJson(EntityUtils.toString(res.getEntity()));
            if (res.getCode() != 201) {
                throw new RuntimeException(body);
            }
            return body;
        });
    }

    @Override
    public CouchDBConnector createConnection() {
        return this;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    private URI resolveUri(String... paths) {
        URIBuilder builder = new URIBuilder()
                .setScheme(httpHost.getSchemeName())
                .setHost(httpHost.getHostName())
                .setPort(httpHost.getPort());

        for (String path : paths) {
            builder.appendPath(path);
        }

        try {
            return builder.build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    static class DesignDoc {
        public String _id;
        public String _rev;
        public Map<String, Map<String, String>> views;

        DesignDoc(String viewName, Map<String, String> view) {
            views = new HashMap<>();
            views.put(viewName, view);
        }
    }
}
