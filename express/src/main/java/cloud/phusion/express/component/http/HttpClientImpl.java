package cloud.phusion.express.component.http;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.EngineFactory;
import cloud.phusion.PhusionException;
import cloud.phusion.express.util.TimeMarker;
import cloud.phusion.protocol.http.HttpClient;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpResponse;
import org.apache.commons.io.IOUtils;

import javax.net.ssl.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpClientImpl implements HttpClient {
    private static final String _position = HttpClientImpl.class.getName();

    private boolean trustAnyHttpsCertificate;
    private boolean returnResponseHeaders;
    private Context ctx = null;

    private HttpMethod reqMethod = null;
    private String reqUrl = null;
    private Map<String, String> reqHeaders = null;
    private DataObject reqBody = null;
    private Context reqCtx = null;

    public HttpClientImpl(Context ctx, boolean trustAnyHttpsCertificate, boolean returnResponseHeaders) {
        super();

        this.trustAnyHttpsCertificate = trustAnyHttpsCertificate;
        this.returnResponseHeaders = returnResponseHeaders;
        this.ctx = ctx==null ? EngineFactory.createContext() : ctx;
    }

    public HttpClientImpl(Context ctx) {
        this(ctx, false, true);
    }

    @Override
    public HttpClient get(String url) {
        _clearRequest();
        reqMethod = HttpMethod.GET;
        reqUrl = url;
        return this;
    }

    @Override
    public HttpClient post(String url) {
        _clearRequest();
        reqMethod = HttpMethod.POST;
        reqUrl = url;
        return this;
    }

    @Override
    public HttpClient put(String url) {
        _clearRequest();
        reqMethod = HttpMethod.PUT;
        reqUrl = url;
        return this;
    }

    @Override
    public HttpClient delete(String url) {
        _clearRequest();
        reqMethod = HttpMethod.DELETE;
        reqUrl = url;
        return this;
    }

    @Override
    public HttpClient header(String header, String value) {
        if (reqHeaders == null) reqHeaders = new HashMap<>();
        reqHeaders.put(header, value);
        return this;
    }

    @Override
    public HttpClient body(DataObject body) {
        reqBody = body;
        return this;
    }

    @Override
    public HttpClient body(String body) {
        reqBody = new DataObject(body);
        return this;
    }

    @Override
    public HttpClient context(Context ctx) {
        reqCtx = ctx;
        return this;
    }

    @Override
    public HttpResponse send() throws Exception {
        HttpResponse result = _request(reqMethod, reqUrl, reqHeaders, reqBody, reqCtx==null ? ctx : reqCtx);

        _clearRequest();
        return result;
    }

    private void _clearRequest() {
        reqMethod = null;
        reqUrl = null;
        reqHeaders = null;
        reqBody = null;
        reqCtx = null;
    }

    private HttpResponse _request(HttpMethod method, String url, Map<String, String> headers, DataObject body, Context ctx) throws Exception {
        ctx.logInfo(_position, "HTTP request",
                String.format("method=%s, url=%s, headers=%s, body=%s", method, url, headers, body==null?"":body.getString(500)));
        TimeMarker marker = new TimeMarker();

        URL objUrl = new URL(url);
        HttpURLConnection conn = (HttpURLConnection) objUrl.openConnection();

        if (conn instanceof HttpsURLConnection && trustAnyHttpsCertificate) {
            // Do not check HTTPS certificates
            ((HttpsURLConnection)conn).setSSLSocketFactory(sslSocketFactory);
            ((HttpsURLConnection)conn).setHostnameVerifier(trustAnyHostnameVerifier);
        }

        conn.setRequestMethod(_translateHttpMethodToString(method));
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);

        if (headers!=null && headers.size()>0) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                conn.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }

        HttpResponse response = new HttpResponse();

        try {
            conn.connect();

            // Send request
            if (body != null) {
                try (OutputStream out = conn.getOutputStream()) {
                    out.write(body.getString().getBytes(StandardCharsets.UTF_8));
                    out.flush();
                }
            }

            // Receive response
            String respBody = null;

            if (conn.getResponseCode() < 400) {
                try (InputStream in = conn.getInputStream()) {
                    respBody = IOUtils.toString(in, StandardCharsets.UTF_8);
                }
            }
            else {
                try (InputStream in = conn.getErrorStream()) {
                    respBody = IOUtils.toString(in, StandardCharsets.UTF_8);
                }
            }

            response.setStatusCode(conn.getResponseCode());
            if (respBody != null) response.setBody(new DataObject(respBody));

            // Retrieve response HTTP headers
            if (returnResponseHeaders) {
                Map<String, List<String>> respHeaders = conn.getHeaderFields();
                for (String key : respHeaders.keySet()) {
                    if (key == null) continue;
                    response.setHeader(key, respHeaders.get(key).get(0)); // Get the first item only
                }
            }
        } catch (Exception ex) {
            throw new PhusionException("HTTP_CLIENT", "Error", ctx, ex);
        } finally {
            try {
                conn.disconnect();
            } catch (Exception ex) {
                throw new PhusionException("HTTP_CLIENT", "Failed to disconnect", ctx, ex);
            }
        }

        double ms = marker.mark();
        ctx.logInfo(_position, "HTTP response", String.format("code=%d, headers=%s, body=%s, time=%.1fms",
                response.getStatusCode(), response.getHeaders(), response.getBody()==null?"":response.getBody().getString(500), ms));

        return response;
    }

    private String _translateHttpMethodToString(HttpMethod m) {
        return m.toString();
    }

    // Handle self-signed HTTPS certificates

    private class TrustAnyHostnameVerifier implements HostnameVerifier {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    private class TrustAnyTrustManager implements X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }
    }

    private static final SSLSocketFactory sslSocketFactory = initSSLSocketFactory();
    private static final TrustAnyHostnameVerifier trustAnyHostnameVerifier = new HttpClientImpl(null).new TrustAnyHostnameVerifier();

    private static SSLSocketFactory initSSLSocketFactory() {
        try {
            TrustManager[] tm = {new HttpClientImpl(null).new TrustAnyTrustManager() };
            SSLContext sslContext = SSLContext.getInstance("TLS", "SunJSSE");
            sslContext.init(null, tm, new java.security.SecureRandom());
            return sslContext.getSocketFactory();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
