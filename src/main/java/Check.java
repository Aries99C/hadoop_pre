import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

public class Check {
    public static void main(String[] args) throws IOException {
        String path = "/output/fill/rating/fill/part-r-00000";
        FileSystem hdfs = FileSystem.get(URI.create(path), new Configuration());
        InputStream stream = hdfs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split("\\|");
            double rating = Double.parseDouble(values[6]);
            if (rating < 0 || rating > 1) {
                System.out.println(rating);
            }
//            String income = values[11];
//            if (income.isEmpty()) {
//                System.out.println("empty income");
//            }
        }
        reader.close();
        stream.close();
    }
}
