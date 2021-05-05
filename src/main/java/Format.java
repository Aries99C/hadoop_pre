import mapper.FormatMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.FormatReducer;

public class Format {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "format");
        // jar
        job.setJarByClass(Format.class);
        // mapper
        job.setMapperClass(FormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("/input/format"));
        // reducer
        job.setReducerClass(FormatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("/output/format"));
        // submit
        job.waitForCompletion(true);
    }
}
