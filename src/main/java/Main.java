import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;


public class Main {

    public static void main(String[] args) {
        String addr = args[0];
        String port = args[1];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + addr + ":" + port);
        //conf.set("fs.defaultFS", "hdfs://localhost:9000");
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fsStatus = fs.listStatus(new Path("/"));
            System.out.println("Files in HDFS root:");
            for (FileStatus status : fsStatus) {
                System.out.println(status.getPath().toString());
            }
            System.out.println("Recursively looking through filesystem:");
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
            while(files.hasNext()) {
                LocatedFileStatus status = files.next();
                System.out.println(status.getPath().toString());
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
