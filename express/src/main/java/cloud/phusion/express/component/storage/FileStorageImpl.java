package cloud.phusion.express.component.storage;

import cloud.phusion.Context;
import cloud.phusion.EngineFactory;
import cloud.phusion.PhusionException;
import cloud.phusion.storage.FileStorage;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;

public class FileStorageImpl implements FileStorage {
    private String myPrivateRootPath = null;
    private String myPublicRootPath = null;
    private String myPublicRootUrl = null;
    private String namespace = null;
    private Context baseCtx = null;

    /**
     * The path seperator is "/".
     *
     * @param namespace {applicationId}、{integrationId}、{clientId}
     */
    public FileStorageImpl(String namespace, String privateRootPath, String publicRootPath, String publicRootUrl, Context ctx) {
        super();

        this.namespace = namespace;
        namespace = (namespace==null || namespace.length()==0) ? "" : "/"+namespace;

        this.myPrivateRootPath = (privateRootPath==null || privateRootPath.length()==0) ? null : privateRootPath + namespace;
        this.myPublicRootPath = (publicRootPath==null || publicRootPath.length()==0) ? null : publicRootPath + namespace;
        this.myPublicRootUrl = (publicRootUrl==null || publicRootUrl.length()==0) ? null : publicRootUrl + namespace;

        this.baseCtx = ctx==null ? EngineFactory.createContext() : ctx;
    }

    public FileStorageImpl(String namespace, String privateRootPath, String publicRootPath, String publicRootUrl) {
        this(namespace, privateRootPath, publicRootPath, publicRootUrl, null);
    }

    public FileStorageImpl(String privateRootPath, Context ctx) {
        this(null, privateRootPath, null, null, ctx);
    }

    public FileStorageImpl(String privateRootPath) {
        this(null, privateRootPath, null, null, null);
    }

    public FileStorageImpl(String namespace, String privateRootPath) {
        this(namespace, privateRootPath, null, null, null);
    }

    public FileStorageImpl(String namespace, String publicRootPath, String publicRootUrl) {
        this(namespace, null, publicRootPath, publicRootUrl, null);
    }

    @Override
    public boolean doesFileExist(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to check file", ctx);
        return _doesFileExists( myPrivateRootPath + path, ctx );
    }

    @Override
    public boolean doesPublicFileExist(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to check file", ctx);
        return _doesFileExists( myPublicRootPath + path, ctx );
    }

    @Override
    public boolean doesFileExist(String path) throws Exception {
        return doesFileExist(path, baseCtx);
    }

    @Override
    public boolean doesPublicFileExist(String path) throws Exception {
        return doesPublicFileExist(path, baseCtx);
    }

    private boolean _doesFileExists(String path, Context ctx) throws Exception {
        _checkPathValidity(path, "find file", ctx);

        File f = new File(path);
        return f.exists();
    }

    @Override
    public long getFileSize(String path) throws Exception {
        return getFileSize(path, baseCtx);
    }

    @Override
    public long getPublicFileSize(String path) throws Exception {
        return getPublicFileSize(path, baseCtx);
    }

    @Override
    public long getFileSize(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to get file size", ctx);
        return _getFileSize( myPrivateRootPath + path, ctx );
    }

    @Override
    public long getPublicFileSize(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to get file size", ctx);
        return _getFileSize( myPublicRootPath + path, ctx );
    }

    private long _getFileSize(String path, Context ctx) throws Exception {
        _checkPathValidity(path, "find file", ctx);

        File f = new File(path);

        // For folders, return 0; If not found the path, return -1
        return f.exists() ? (f.isFile() ? f.length() : 0) : -1;
    }

    @Override
    public String[] listFolders(String path) throws Exception {
        return listFolders(path, baseCtx);
    }

    @Override
    public String[] listPublicFolders(String path) throws Exception {
        return listPublicFolders(path, baseCtx);
    }

    @Override
    public String[] listFiles(String path) throws Exception {
        return listFiles(path, baseCtx);
    }

    @Override
    public String[] listPublicFiles(String path) throws Exception {
        return listPublicFiles(path, baseCtx);
    }

    @Override
    public String[] listFolders(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to list folders/files", ctx);
        return _listFiles( myPrivateRootPath + path, false, ctx );
    }

    @Override
    public String[] listPublicFolders(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to list folders/files", ctx);
        return _listFiles( myPublicRootPath + path, false, ctx );
    }

    @Override
    public String[] listFiles(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to list folders/files", ctx);
        return _listFiles( myPrivateRootPath + path, true, ctx );
    }

    @Override
    public String[] listPublicFiles(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to list folders/files", ctx);
        return _listFiles( myPublicRootPath + path, true, ctx );
    }

    private String[] _listFiles(String path, boolean listFiles, Context ctx) throws Exception {
        _checkPathValidity(path, "list folders/files", ctx);

        File folder = new File(path);

        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            ArrayList<String> result = new ArrayList<>();

            for (File f : files) {
                if (listFiles && f.isFile() || !listFiles && f.isDirectory()) result.add(f.getName());
            }

            return result.toArray(new String[]{});
        }
        else
            return null;
    }

    @Override
    public void saveToFile(String path, InputStream content) throws Exception {
        saveToFile(path, content, baseCtx);
    }

    @Override
    public void saveToPublicFile(String path, InputStream content) throws Exception {
        saveToPublicFile(path, content, baseCtx);
    }

    @Override
    public void saveToFile(String path, InputStream content, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to save file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[private]"+path);

        _saveToFile( myPrivateRootPath + path, content, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
    }

    @Override
    public void saveToPublicFile(String path, InputStream content, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to save file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[public]"+path);

        _saveToFile( myPublicRootPath + path, content, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
}

    private void _saveToFile(String path, InputStream content, Context ctx) throws Exception {
        _checkPathValidity(path, "save file", ctx);

        File f = new File(path);

        if (path.charAt(path.length()-1)=='/' || f.isDirectory())
            throw new PhusionException("FILE_NOT", "Failed to save file", ctx);

        if (! f.exists()) {
            // Automatically create the parent folders
            try {
                File folder = new File(f.getParent());
                if (! folder.exists()) folder.mkdirs();
            } catch (Exception ex) {
                throw new PhusionException("FS_OP", "Failed to create folders", ctx, ex);
            }
        }

        try (FileOutputStream out = new FileOutputStream(f, false)) {
            int len;
            byte[] bytes = new byte[1024];

            while ((len = content.read(bytes)) != -1) {
                out.write(bytes, 0, len);
            }
        } catch (Exception ex) {
            throw new PhusionException("FS_OP", "Failed to write file", ctx, ex);
        }
    }

    @Override
    public void saveToFile(String path, byte[] content) throws Exception {
        saveToFile(path, content, baseCtx);
    }

    @Override
    public void saveToPublicFile(String path, byte[] content) throws Exception {
        saveToPublicFile(path, content, baseCtx);
    }

    @Override
    public void saveToFile(String path, byte[] content, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to save file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[private]"+path);

        _saveToFile( myPrivateRootPath + path, content, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
    }

    @Override
    public void saveToPublicFile(String path, byte[] content, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to save file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[public]"+path);

        _saveToFile( myPublicRootPath + path, content, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
    }

    private void _saveToFile(String path, byte[] content, Context ctx) throws Exception {
        try (ByteArrayInputStream in = new ByteArrayInputStream(content)) {
            _saveToFile(path, in, ctx);
        }
    }

    @Override
    public InputStream readFromFile(String path) throws Exception {
        return readFromFile(path, baseCtx);
    }

    @Override
    public InputStream readFromPublicFile(String path) throws Exception {
        return readFromPublicFile(path, baseCtx);
    }

    @Override
    public InputStream readFromFile(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to access file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[private]"+path);

        InputStream result = _readFromFile( myPrivateRootPath + path, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
        return result;
    }

    @Override
    public InputStream readFromPublicFile(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to access file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[public]"+path);

        InputStream result = _readFromFile( myPublicRootPath + path, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
        return result;
    }

    private InputStream _readFromFile(String path, Context ctx) throws Exception {
        _checkPathValidity(path, "read file", ctx);

        File f = new File(path);

        if (! f.exists()) {
            throw new PhusionException("FILE_NONE", "Failed to access file", ctx);
        }
        else if (path.charAt(path.length()-1)=='/' || f.isDirectory()) {
            throw new PhusionException("FILE_NOT", "Failed to access file", ctx);
        }

        try {
            return new FileInputStream(f);
        } catch (Exception ex) {
            throw new PhusionException("FS_OP", "Failed to read file", ctx, ex);
        }
    }

    @Override
    public byte[] readAllFromFile(String path) throws Exception {
        return readAllFromFile(path, baseCtx);
    }

    @Override
    public byte[] readAllFromPublicFile(String path) throws Exception {
        return readAllFromPublicFile(path, baseCtx);
    }

    @Override
    public byte[] readAllFromFile(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to access file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[private]"+path);

        byte[] result = _readAllFromFile( myPrivateRootPath + path, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
        return result;
    }

    @Override
    public byte[] readAllFromPublicFile(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to access file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[public]"+path);

        byte[] result = _readAllFromFile( myPublicRootPath + path, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
        return result;
    }

    private byte[] _readAllFromFile(String path, Context ctx) throws Exception {
        _checkPathValidity(path, "read file", ctx);

        byte[] result;

        try (InputStream in = _readFromFile(path, ctx)) {
            result = new byte[(int)_getFileSize(path, ctx)];

            int offset = 0;
            while (offset < result.length) {
                int bytes = in.read(result, offset, result.length - offset);
                if (bytes == -1) break;
                offset += bytes;
            }

            return result;
        } catch (Exception ex) {
            throw new PhusionException("FS_OP", "Failed to read file", ctx, ex);
        }
    }

    @Override
    public void removeFile(String path) throws Exception {
        removeFile(path, baseCtx);
    }

    @Override
    public void removePublicFile(String path) throws Exception {
        removePublicFile(path, baseCtx);
    }

    @Override
    public void removeFile(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to remove file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[private]"+path);

        _removeFile( myPrivateRootPath + path, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
    }

    @Override
    public void removePublicFile(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to remove file", ctx);

        ctx.setContextInfo("namespace", namespace);
        ctx.setContextInfo("path", "[public]"+path);

        _removeFile( myPublicRootPath + path, ctx );

        ctx.removeContextInfo("path");
        ctx.removeContextInfo("namespace");
    }

    private void _removeFile(String path, Context ctx) throws Exception {
        _checkPathValidity(path, "remove file", ctx);

        boolean removed = true;

        try {
            File f = new File(path);
            if (path.charAt(path.length()-1)=='/' || f.isDirectory()) FileUtils.deleteDirectory(f);
            else removed = f.delete();
        } catch (Exception ex) {
            throw new PhusionException("FS_OP", "Failed to delete path or file", ctx, ex);
        }

        if (! removed) throw new PhusionException("FS_OP", "Failed to delete path or file", ctx);
    }

    @Override
    public void removeAll() throws Exception {
        removeAll(baseCtx);
    }

    @Override
    public void removeAll(Context ctx) throws Exception {
        File f;

        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to remove files", ctx);

        try {
            f = new File(myPrivateRootPath);
            FileUtils.deleteDirectory(f);
        } catch (Exception ex) {
            throw new PhusionException("FS_OP", "Failed to delete private root", ctx, ex);
        }

        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to remove files", ctx);

        try {
            f = new File(myPublicRootPath);
            FileUtils.deleteDirectory(f);
        } catch (Exception ex) {
            throw new PhusionException("FS_OP", "Failed to delete public root", ctx, ex);
        }
    }

    @Override
    public String getPublicFileUrl(String path) throws Exception {
        if (myPublicRootUrl == null) throw new PhusionException("FS_NONE", "Failed to get file url", "path="+path);
        return myPublicRootUrl + path;
    }

    private void _checkPathValidity(String path, String op, Context ctx) throws Exception {
        if (path.indexOf("..") >= 0) throw new PhusionException("FILE_INVALID", "Failed to "+op, "path="+path);
    }

    @Override
    public FileProperties getFileProperties(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to get file properties", ctx);
        return _getFileProperties( myPrivateRootPath + path, ctx );
    }

    @Override
    public FileProperties getPublicFileProperties(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to get file properties", ctx);
        return _getFileProperties( myPublicRootPath + path, ctx );
    }

    private FileProperties _getFileProperties(String path, Context ctx) throws Exception {
        _checkPathValidity(path, "find file", ctx);

        File f = new File(path);

        if (f.exists()) return _fileToFileProperties(f);
        else return null;
    }

    @Override
    public FileProperties[] listFilesWithProperties(String path, Context ctx) throws Exception {
        if (myPrivateRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to list files", ctx);
        return _listFilesWithProperties( myPrivateRootPath + path, ctx );
    }

    @Override
    public FileProperties[] listPublicFilesWithProperties(String path, Context ctx) throws Exception {
        if (myPublicRootPath == null)
            throw new PhusionException("FS_NONE", "Failed to list files", ctx);
        return _listFilesWithProperties( myPublicRootPath + path, ctx );
    }

    private FileProperties[] _listFilesWithProperties(String path, Context ctx) throws Exception {
        _checkPathValidity(path, "list folders/files", ctx);

        File folder = new File(path);

        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            ArrayList<FileProperties> result = new ArrayList<>();

            for (File f : files) result.add(_fileToFileProperties(f));
            return result.toArray(new FileProperties[]{});
        }
        else
            return null;
    }

    private FileProperties _fileToFileProperties(File f) {
        FileProperties props = new FileProperties();
        props.name = f.getName();
        props.size = f.length();
        props.isFolder = f.isDirectory();
        props.updateTime = f.lastModified();
        return props;
    }

}
