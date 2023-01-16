package sqlancer.cnosdb.client;


import com.arangodb.internal.util.IOUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class CnosDBClient {
    private final String userName;
    private final String password;
    private final String host;
    private final int port;

    private final String database;

    public CnosDBClient(String host, int port, String userName, String password, String database) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.database = database;
        this.client = HttpClientBuilder.create().build();
    }

    private String url() {
        String url = "http://" + host + ":" + port + "/api/v1/";
        return url;
    }

    public String ping() throws Exception {
        HttpGet httpGet = new HttpGet(this.url() + "ping");
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, getAuth());
        CloseableHttpResponse resp = client.execute(httpGet);

        String content = IOUtils.toString(resp.getEntity().getContent());
        resp.close();
        return content;
    }

    public CnosDBResultSet executeQuery(String query) throws Exception {
        if (!query.endsWith(";")) {
            query = query + ";";
        }
        HttpUriRequest request = createRequest(query);
        CloseableHttpResponse resp = client.execute(request);
        String text = IOUtils.toString(resp.getEntity().getContent());
        if (resp.getStatusLine().getStatusCode() != 200) {
            resp.close();
            throw new Exception(database + ":" + query + "\n" + text);
        }
        resp.close();
        InputStream stream = new ByteArrayInputStream(text.getBytes
                (Charset.forName("UTF-8")));

        return new CnosDBResultSet(new InputStreamReader(stream));
    }

    public boolean execute(String query) throws Exception {
        HttpUriRequest request = createRequest(query);
        CloseableHttpResponse resp = client.execute(request);
        if (resp.getStatusLine().getStatusCode() != 200) {
            String res = IOUtils.toString(resp.getEntity().getContent());
            resp.close();
            throw new Exception(query + res);
        }
        resp.close();
        return true;
    }

    public void close() throws IOException {
        client.close();
    }

    public String getDatabase() {
        return this.database;
    }

    private String getAuth() {
        String auth = userName + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(
                auth.getBytes(StandardCharsets.ISO_8859_1));
        return "Basic " + new String(encodedAuth);

    }

    private HttpUriRequest createRequest(String query) throws Exception {

        URIBuilder builder = new URIBuilder(this.url() + "sql");
        builder.setParameter("db", database);
        builder.setParameter("pretty", "true");
        HttpPost httpPost = new HttpPost(builder.build());

        httpPost.setHeader(HttpHeaders.AUTHORIZATION, getAuth());
        StringEntity stringEntity = new StringEntity(query);
        httpPost.setEntity(stringEntity);
        return httpPost;
    }

    private CloseableHttpClient client;

}
