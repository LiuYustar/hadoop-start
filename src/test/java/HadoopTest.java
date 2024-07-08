import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class HadoopTest {
    private FileSystem fs;

    @Before
    public void getFileSystemTest() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020");
        fs = FileSystem.get(conf);
        System.out.println(fs.getClass().getName());
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

//    @Test
//    public void uploadFileTest() throws IOException {
//        boolean mkdirs = fs.mkdirs(new Path("/test"));
//        System.out.println(mkdirs);
//    }
//
//    @Test
//    public void deleteFileTest() throws IOException {
//        boolean delete = fs.delete(new Path("/test"), true);
//        System.out.println(delete);
//    }

//    @Test
//    public void listFilesStatusTest() throws IOException {
//        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/input"), true);
//        while (locatedFileStatusRemoteIterator.hasNext()) {
//            System.out.println(locatedFileStatusRemoteIterator.next());
//        }
//
//    }




}
