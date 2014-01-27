package org.idryman;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DemoRun extends Configured implements Tool {

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new DemoRun(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    //conf.set("mapreduce.fieldoutput.header", "ct_audit,ct_action");
    Job job = new Job(conf);
    job.setJobName("test TextWithPath Input");
    job.setJarByClass(DemoRun.class);
    TWPInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(TWPInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }

  public static class CTMapper extends Mapper<LongWritable, TextWithPath, Text, NullWritable>{
    
    @Override
    public void map(LongWritable key, TextWithPath val, Context context) throws IOException, InterruptedException{
      context.write(new Text(val.getPath().toString()), NullWritable.get());
    }
  }

}
