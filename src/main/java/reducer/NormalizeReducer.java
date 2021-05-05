package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

    private final double max = 102.71;
    private final double min = -261.13;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String line = value.toString();
            String[] words = line.split("\\|");
            if (!words[6].equals("?")) {
                double rating = Double.parseDouble(words[6]);
                rating = (rating - min) / (max - min);
                line = line.replace(words[6], String.valueOf(rating));
                context.write(new Text(line), new Text(""));
            } else {
                context.write(value, new Text(""));
            }
        }
    }
}
