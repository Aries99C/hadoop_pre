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
import java.util.*;

public class IncomeFillReducer extends Reducer<Text, Text, Text, Text> {

    private final List<String[]> fillList = new ArrayList<>();

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
            fillList.add(new String[]{keys[0], keys[1], fillValue});
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
                String filled = "";
                for (String[] words : fillList) {
                    if (nationality.equals(words[0]) && career.equals(words[1])) {
                        filled = words[2];
                    }
                }
                line = line.replace(income, filled);
                context.write(new Text(line), new Text(""));
            } else {
                context.write(value, new Text(""));
            }
        }
    }
}
