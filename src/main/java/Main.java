import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class Main {

    public static void main(String[] args) throws IOException {
        String addr = args[0];
        String port = args[1];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + addr + ":" + port);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fsStatus = fs.listStatus(new Path("/"));
        System.out.println("Files in HDFS:");
        for (FileStatus status : fsStatus) {
            System.out.println(status.getPath().toString());
        }
        return;
    }
}
