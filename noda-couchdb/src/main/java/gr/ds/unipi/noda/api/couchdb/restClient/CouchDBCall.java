package gr.ds.unipi.noda.api.couchdb.restClient;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import okhttp3.Call;
import okhttp3.Response;

import java.io.IOException;
import java.lang.reflect.Type;

public class CouchDBCall<T> {

    private final Call call;
    private final Type type;

    public CouchDBCall(Call call, Class<T> type) {
        this.call = call;
        this.type = type;
    }

    public T execute() throws IOException {
        try (Response response = this.call.execute()) {
            JsonReader reader;
            reader = new JsonReader(response.body().charStream());
            return new Gson().fromJson(reader, this.type);
        }
    }

}
