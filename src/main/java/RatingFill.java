import mapper.RatingFillMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.RatingFillReducer;

import java.io.IOException;

public class RatingFill {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "rating fill");
        // jar
        job.setJarByClass(RatingFill.class);
        // mapper
        job.setMapperClass(RatingFillMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("/input/fill/rating/fill"));
        // reducer
        job.setReducerClass(RatingFillReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/fill/rating/fill"));
        // submit
        job.waitForCompletion(true);
    }
}
