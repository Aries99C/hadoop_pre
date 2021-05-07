import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Text {
    public static void main(String[] args) throws IOException {
        Map<String[], String> fillMap = new HashMap<>();
        String path = "/output/fill/income/fill/part-r-00000";
        FileSystem hdfs = FileSystem.get(URI.create(path), new Configuration());
        InputStream stream = hdfs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split("\\|");
            String income = values[11];
            if (income.contains("?")) {
                System.out.println("?");
            }
        }
        reader.close();
        stream.close();
    }
}
