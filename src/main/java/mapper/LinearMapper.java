package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LinearMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split("\\|");
        String career = values[10];
        String rating = values[6];
        if (!rating.contains("?")) {
            context.write(new Text(career), value);
        }
    }
}
