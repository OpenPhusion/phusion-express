package cloud.phusion.express.component.http;

import cloud.phusion.DataObject;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * File upload support:
 * - multipart/form-data: multiple files with filenames
 * - Other binary content type: single file without filename (empty string)
 */
public class HttpPhusionController extends HttpServlet {

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Match the registered HttpServer

        String resultPath = null;
        Map<String, String> resultParams = new HashMap<String, String>();

        String[] pathParts = req.getRequestURI().split("/");
        Map<String, String[]> paths = HttpServerManager.getPaths();

        for (String key : paths.keySet()) {
            if (_isPathMatched(pathParts, paths.get(key), resultParams)) {
                resultPath = key;
                break;
            }
        }

        if (resultPath == null) {
            resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
            PrintWriter w = resp.getWriter();
            w.print("No HttpServer found");
            w.flush();
            return;
        }

        // Compose HTTP Request

        HttpRequest resultReq;

        try {
            resultReq = _buildHttpRequest(req, resultPath, resultParams);
        } catch (Exception ex) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            PrintWriter w = resp.getWriter();
            w.print("Failed to parse request: " + ex.getMessage());
            w.flush();
            return;
        }

        // Run the HttpServer

        HttpResponse resultResp = new HttpResponse();

        try {
            HttpServerManager.runServer(resultPath, resultReq, resultResp);
        } catch (Exception ex) {
            resultResp.setStatusCode(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resultResp.setBody(new DataObject("Error: " + ex.getMessage()));
        } finally {
            // Close all streams if any
            if (resultReq!=null && resultReq.hasFiles()) {
                Set<String> files = resultReq.getFileNames();
                for (String file : files) {
                    try {
                        resultReq.getFileContent(file).close();
                    } catch (Exception ex) {}
                }
            }
        }

        // Compose Response

        int status = resultResp.getStatusCode();
        resp.setStatus(status);

        Map<String, String> resultHeaders = resultResp.getHeaders();
        for (String key : resultHeaders.keySet()) {
            resp.setHeader(key, resultHeaders.get(key));
        }

        String resultRespBody = resultResp.getBody()==null ? null : resultResp.getBody().getString();
        if (resultRespBody!=null && resultRespBody.length()>0) {
            PrintWriter w = resp.getWriter();
            w.print(resultRespBody);
            w.flush();
        }
    }

    private boolean _isPathMatched(String[] pathParts, String[] patternParts, Map<String, String> params) {
        boolean matched = false;
        int patternPartsLen = patternParts.length;

        if (pathParts.length != patternPartsLen) matched = false;
        else {
            matched = true;
            for (int i = 0; i < patternPartsLen; i++) {
                String part = patternParts[i];

                if (part.length()>2 && part.charAt(0)=='{') {
                    params.put(part.substring(1,part.length()-1), pathParts[i]);
                }
                else {
                    if (!part.equals(pathParts[i])) {
                        matched = false;
                        break;
                    }
                }
            }
        }

        return matched;
    }

    private HttpRequest _buildHttpRequest(HttpServletRequest req, String resultPath, Map<String, String> params) throws Exception {
        HttpMethod method = HttpMethod.valueOf(req.getMethod().toUpperCase());
        Map<String, String> headers = new HashMap<String, String>();

        Enumeration<String> headerNames = req.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String header = headerNames.nextElement();
            headers.put(header, req.getHeader(header));
        }

        Enumeration<String> paramNames = req.getParameterNames();
        while (paramNames.hasMoreElements()) {
            String param = paramNames.nextElement();
            params.put(param, req.getParameter(param));
        }

        String contentType = req.getContentType();
        if (contentType == null) contentType = "text/plain";
        int pos = contentType.indexOf(';');
        if (pos > 0) contentType = contentType.toLowerCase().substring(0, pos);

        switch (contentType) {
            case "multipart/form-data":
                ServletFileUpload upload = new ServletFileUpload( new DiskFileItemFactory());
                List<FileItem> list = upload.parseRequest(req);
                Map<String, InputStream> files = new HashMap<>();

                if (list != null) {
                    for (FileItem item : list) {
                        if (item.isFormField()) {
                            // Ordinary form field
                            params.put(item.getFieldName(), item.getString("UTF-8"));
                        } else {
                            // File
                            String fileName = item.getName();
                            if (fileName!=null && fileName.length()>0) // Ignore the empty file
                                files.put(item.getName(), item.getInputStream());
                        }
                    }
                }

                return new HttpRequest(method, resultPath, headers, params, files);

            case "application/json":
            case "text/plain":
            case "text/html":
            case "text/xml":
            case "application/xml":
                String body = IOUtils.toString(req.getInputStream(), StandardCharsets.UTF_8);
                return new HttpRequest(method, resultPath, headers, params, new DataObject(body));

            default:
                Map<String, InputStream> file = new HashMap<>();
                file.put("", req.getInputStream());
                return new HttpRequest(method, resultPath, headers, params, file);
        }
    }

}
