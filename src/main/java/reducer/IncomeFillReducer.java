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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IncomeFillReducer extends Reducer<Text, Text, Text, Text> {

    private final Map<String[], String> fillMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        String path = "/output/fill/income/average/part-r-00000";
        FileSystem hdfs = FileSystem.get(URI.create(path), new Configuration());
        InputStream stream = hdfs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] keyword = line.split("\\s+");
            String[] keys = keyword[0].split(",");
            String fillValue = keyword[1].substring(0,4);
            fillMap.put(keys, fillValue);
        }
        reader.close();
        stream.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String line = value.toString();
            String[] attributes = line.split("\\|");
            String income = attributes[11];
            if (income.contains("?")) {
                String nationality = attributes[9];
                String career = attributes[10];
                String[] keys = new String[2];
                keys[0] = nationality;
                keys[1] = career;
                String filled = "";
                if (fillMap.get(keys) != null) {
                    filled = fillMap.get(keys);
                }
                line = line.replace(income, filled);
                context.write(new Text(line), new Text(""));
            } else {
                context.write(value, new Text(""));
            }
        }
    }
}
