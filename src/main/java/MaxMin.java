import mapper.MaxMinMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.MaxMinReducer;

public class MaxMin {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxmin");
        // jar
        job.setJarByClass(MaxMin.class);
        // mapper
        job.setMapperClass(MaxMinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/input/maxmin"));
        // reducer
        job.setReducerClass(MaxMinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/maxmin"));
        // submit
        job.waitForCompletion(true);
    }
}
