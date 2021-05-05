package reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxMinReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private double max = Double.MIN_VALUE;
    private double min = Double.MAX_VALUE;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
        if ("MAX".equals(key.toString())) {
            for (DoubleWritable value : values) {
                if (value.get() > max) {
                    max = value.get();
                }
            }
        }
        if ("MIN".equals(key.toString())) {
            for (DoubleWritable value : values) {
                if (value.get() < min) {
                    min = value.get();
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("MAX"), new DoubleWritable(max));
        context.write(new Text("MIN"), new DoubleWritable(min));
    }
}
