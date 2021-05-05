package mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxMinMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private double max = Double.MIN_VALUE;
    private double min = Double.MAX_VALUE;

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String line = value.toString();
        String[] values = line.split("\\|");
        if (!values[6].equals("?")) {
            double rating = Double.parseDouble(values[6]);
            if (rating > max) {
                max = rating;
            }
            if (rating < min) {
                min = rating;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("MAX"), new DoubleWritable(max));
        context.write(new Text("MIN"), new DoubleWritable(min));
    }
}
