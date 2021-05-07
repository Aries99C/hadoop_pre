import mapper.IncomeFillMapper;
import mapper.SamplingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.IncomeFillReducer;
import reducer.SamplingReducer;

import java.io.IOException;

public class IncomeFill {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "income fill");
        // jar
        job.setJarByClass(IncomeFill.class);
        // mapper
        job.setMapperClass(IncomeFillMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("/input/fill/income/fill"));
        // reducer
        job.setReducerClass(IncomeFillReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/fill/income/fill"));
        // submit
        job.waitForCompletion(true);
    }
}
