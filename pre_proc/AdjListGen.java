package pre;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * adjList_pre_processV2
 * Created by Yao Wang.
 */
public class AdjListGen extends Configured implements Tool {
  private static final int REDUCER_NUM = 55;
  private static final int MAX_ID = 124836179;
  private static final int MIN_ID = 101;
  private static final int REMAINDER = (MAX_ID - MIN_ID + 1) % REDUCER_NUM;
  private static final int QUOTIENT = (MAX_ID - MIN_ID + 1) / REDUCER_NUM;
  private static final int MID_ID = REMAINDER * (QUOTIENT + 1);

  static class EdgeMapper extends Mapper<Object, Text, IntWritable, Text> {
    IntWritable node1Int = new IntWritable();
    IntWritable node2Int = new IntWritable();

    Text node1Text = new Text();
    Text node2Text = new Text();
    @Override
    protected void map(Object key, Text value,
                       Context context) throws IOException, InterruptedException {
      String inputEdge = value.toString();
      try {
        if (inputEdge.length() > 0 && inputEdge.charAt(0) != '#') {
          String[] nodes = inputEdge.split("\\s+");

          if (nodes.length != 2) {
            throw new IllegalArgumentException("input Edge's format is wrong: " + inputEdge + '\n');
          } else {
            node1Int.set(Integer.parseInt(nodes[0]));
            node2Int.set(Integer.parseInt(nodes[1]));
            node1Text.set(nodes[0]);
            node2Text.set(nodes[1]);

            context.write(node1Int, node2Text);
            context.write(node2Int, node1Text);
          }
        }
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      }
    }
  }

  public static class AdjListReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
      for (Text nextNode : values) {
        sb.append(nextNode.toString()).append(',');
      }
      sb.deleteCharAt(sb.length() - 1);
      context.write(key, new Text(sb.toString()));
    }
  }

  public static class IdPartitioner extends Partitioner<IntWritable, Text> {


    @Override
    public int getPartition(IntWritable key, Text value, int reduceTaskNum) {
      int idNum = key.get() - MIN_ID;
      if (idNum < MID_ID) {
        return idNum / (QUOTIENT + 1);
      } else {
        return (idNum - MID_ID) / QUOTIENT + REMAINDER;
      }
    }
  }


  @Override
  public int run(String[] strings) throws Exception {
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "AdjList _ total order");

    String[] otherArgs = new GenericOptionsParser(conf, strings).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: EdgeList-pre-process <in> <out>");
      return 2;
    }
    Path inputPath =  new Path(otherArgs[0]);
    Path outputPath =  new Path(otherArgs[1]);

/*    //global variables setting:
    conf.set("reduceNum", otherArgs[2]);
    conf.set("maxId", otherArgs[3]);
    conf.set("minId", otherArgs[4]);
    conf.set("quotient", String.valueOf(quotient));
    conf.set("remainder", String.valueOf(remainder));
    conf.set("midId", String.valueOf(minId));*/

    job.setJarByClass(AdjListGen.class);
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setNumReduceTasks(REDUCER_NUM);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setPartitionerClass(IdPartitioner.class);
    job.setMapperClass(EdgeMapper.class);
    job.setCombinerClass(AdjListReducer.class);
    job.setReducerClass(AdjListReducer.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }


  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AdjListGen(), args);
    System.exit(exitCode);
  }

}
