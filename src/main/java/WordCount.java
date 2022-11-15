import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WordCount {
  public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{
    private final Text valueText = new Text();
    private final Text keyText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      if (!itr.hasMoreTokens()) return;

      valueText.set(itr.nextToken());

      while (itr.hasMoreTokens()) {
        keyText.set(itr.nextToken());
        context.write(keyText, valueText);
      }
    }
  }
  public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

      for (Text val : values) {
        sum += val.get();
      }
      result.set("");
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}