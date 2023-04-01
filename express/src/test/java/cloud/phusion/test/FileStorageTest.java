package cloud.phusion.test;

import cloud.phusion.storage.FileStorage;
import org.junit.*;

public class FileStorageTest {
    private static FileStorage storage;

    @BeforeClass
    public static void setUp() throws Exception {
//        String base = FileStorageTest.class.getClassLoader().getResource("").getPath();
//
//        Properties props = new Properties();
//        props.setProperty(EngineFactory.FileStorage_PrivateRootPath, base + "FileStorage/private");
//        props.setProperty(EngineFactory.FileStorage_PublicRootPath, base + "FileStorage/public");
//        props.setProperty(EngineFactory.FileStorage_PublicRootUrl, "https://phusion.cloud/filestorage");
//
//        Engine engine = EngineFactory.createEngine(props);
//
//        storage = engine.getFileStorageForIntegration("ISimple");
    }

    @Test
    public void testCheckFileExists() throws Exception {
//        assertTrue( storage.doesFileExist("/a") );
//        assertTrue( storage.doesFileExist("/a/example.txt") );
    }

    @Test
    public void testGetFileSize() throws Exception {
//        assertEquals( storage.getFileSize("/a"), 0 );
//        assertTrue( storage.getFileSize("/a/example.txt") > 0 );
//        assertEquals( storage.getFileSize("/b"), -1 );
    }

    @Test
    public void testListFiles() throws Exception {
//        assertEquals( 2, storage.listFiles("/a").length );
//        assertEquals( 0, storage.listFolders("/a").length );
//        assertEquals( "a", storage.listFolders("/")[0] );
//        assertEquals( 1, storage.listFolders("/").length );
//        assertEquals( 0, storage.listFiles("/").length );
    }

    @Test
    public void testGetFileProperties() throws Exception {
//        FileStorage.FileProperties f = storage.getFileProperties("/a", null);
//        System.out.println(f);
    }

    @Test
    public void testListFileProperties() throws Exception {
//        FileStorage.FileProperties[] f = storage.listFilesWithProperties("/a", null);
//        System.out.println(Arrays.toString(f));
    }

    @Test
    public void testReadFile() throws Exception {
//        assertTrue( storage.readAllFromFile("/a/example.txt").length > 0 );
    }

    @Test
    public void testFileIO() throws Exception {
//        String str = "The complete developer platform to build, scale, and deliver secure software.";
//
//        storage.saveToPublicFile("/b/haha.txt", str.getBytes(StandardCharsets.UTF_8) );
//        assertTrue( storage.doesPublicFileExist("/b/haha.txt") );
//        assertTrue( storage.readAllFromPublicFile("/b/haha.txt").length > 0 );
//
//        storage.removePublicFile("/b/haha.txt");
//        assertFalse( storage.doesPublicFileExist("/b/haha.txt") );
    }

    @Test
    public void testRemoveFile() throws Exception {
//        storage.removeAll();
    }

    @AfterClass
    public static void tearDown() {
    }
}
