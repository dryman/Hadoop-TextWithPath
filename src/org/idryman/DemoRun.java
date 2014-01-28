package org.idryman;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DemoRun extends Configured implements Tool {


  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new DemoRun(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf);
    job.setJobName("test TextWithPath Input");
    job.setJarByClass(DemoRun.class);
    
    TWPInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(TWPInputFormat.class);
    job.setMapperClass(TestMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }

  public static class TestMapper extends Mapper<LongWritable, TextWithPath, Text, IntWritable>{
    
    /**
     * Only override `run` instead of `map` method; because we just want to see one output
     * per mapper, instead of printing every line.
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException{
      context.nextKeyValue();
      TextWithPath twp = context.getCurrentValue();
      context.write(new Text(twp.getPath().toString()), new IntWritable(1));
    }
  }

}
