package reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

public class RatingFillReducer extends Reducer<Text, Text, Text, Text> {

    double x0, x1, x2, x3, x4;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String path = "/output/fill/rating/linear/part-r-00000";
        FileSystem hdfs = FileSystem.get(URI.create(path), new Configuration());
        InputStream stream = hdfs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            x0 = Double.parseDouble(values[0]);
            x1 = Double.parseDouble(values[1]);
            x2 = Double.parseDouble(values[2]);
            x3 = Double.parseDouble(values[3]);
            x4 = Double.parseDouble(values[4]);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String line = value.toString();
            String[] attributes = line.split("\\|");
            String rating = attributes[6];
            if (rating.contains("?")) {
                double longitude = Double.parseDouble(attributes[1]);
                double latitude = Double.parseDouble(attributes[2]);
                double altitude = Double.parseDouble(attributes[3]);
                double income = Double.parseDouble(attributes[11]);
                double fillValue = x0 + x1*longitude + x2*latitude + x3*altitude + x4*income;
                String filled = String.valueOf(fillValue);
                line = line.replace(rating, filled);
                context.write(new Text(line), new Text(""));
            } else {
                context.write(value, new Text(""));
            }
        }
    }
}
