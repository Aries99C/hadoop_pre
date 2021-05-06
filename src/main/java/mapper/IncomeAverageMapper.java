package mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IncomeAverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split("\\|");
        String nationality = values[9];
        String career = values[10];
        String income = values[11];
        if (!income.contains("?")) {
            context.write(new Text(nationality + "," + career), new DoubleWritable(Double.parseDouble(income)));
        }
    }
}
