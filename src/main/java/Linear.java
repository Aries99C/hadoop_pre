import mapper.LinearMapper;
import mapper.SamplingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.LinearReducer;
import reducer.SamplingReducer;

import java.io.IOException;

public class Linear {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "linear");
        // jar
        job.setJarByClass(Linear.class);
        // mapper
        job.setMapperClass(LinearMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("/input/fill/rating/linear"));
        // reducer
        job.setReducerClass(LinearReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/fill/rating/linear"));
        // submit
        job.waitForCompletion(true);
    }
}
