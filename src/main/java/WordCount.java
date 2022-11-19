import java.io.IOException;
import java.util.Iterator;
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
      StringTokenizer outerIterator = new StringTokenizer(value.toString());

      if (outerIterator.hasMoreTokens())
        valueText.set(outerIterator.nextToken());

      for (int i = 1; outerIterator.hasMoreTokens(); i++) {
        String outerToken = outerIterator.nextToken();
        StringTokenizer innerIterator = new StringTokenizer(value.toString());

        innerIterator.nextToken();
        for (int j = 1; innerIterator.hasMoreTokens(); j++) {
          if (i == j || i > j) continue;
          StringBuilder sb = new StringBuilder();
          String innerToken = innerIterator.nextToken();
          sb.append(outerToken).append(",").append(innerToken);

          keyText.set(sb.toString());
          context.write(keyText, valueText);
        }
      }
    }
  }
  public static class TextReducer extends Reducer<Text,Text,Text,Text> {
    private final Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

      Iterator<Text> iterator = values.iterator();
      if (!iterator.hasNext()) return;

      StringBuilder sb = new StringBuilder();

      while (iterator.hasNext()){
        sb.append(iterator.next());
        if (iterator.hasNext())
          sb.append(",");
      }

      result.set(sb.toString());
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(TextReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}