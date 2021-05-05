package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String line = value.toString();
            String[] params = line.split("\\|");
            double longitude = Double.parseDouble(params[1]);
            double latitude = Double.parseDouble(params[2]);
            if (8.1461259 <= longitude && longitude <= 11.1993265 && 56.5824856 <= latitude && latitude <= 57.750511) {
                context.write(value, new Text(""));
            }
        }
    }
}
