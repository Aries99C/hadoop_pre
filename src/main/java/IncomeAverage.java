import mapper.IncomeAverageMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.IncomeAverageReducer;

import java.io.IOException;

public class IncomeAverage {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "income average");
        // jar
        job.setJarByClass(IncomeAverage.class);
        // mapper
        job.setMapperClass(IncomeAverageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/input/fill/income/average"));
        // reducer
        job.setReducerClass(IncomeAverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/fill/income/average"));
        // submit
        job.waitForCompletion(true);
    }
}
