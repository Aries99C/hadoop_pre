package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

public class SamplingReducer extends Reducer<Text, Text, Text, Text> {
    public static final Random random = new Random();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            if (random.nextInt(10000) < 10) {
                context.write(value, new Text(""));
            }
        }
    }
}
