package org.idryman;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class TWPRecordReader extends LineRecordReader{
  private TextWithPath value = null;
  private Path path = null;
  
  public TWPRecordReader(byte[] recordDelimiterBytes) {
    super(recordDelimiterBytes);
  }

  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException{
    super.initialize(genericSplit, context);
    FileSplit split = (FileSplit) genericSplit;
    path = split.getPath();
  }
  
  public boolean nextKeyValue() throws IOException {
    if (super.nextKeyValue()){
      if (value == null)
        value = new TextWithPath(path);
      value.set(super.getCurrentValue());
      return true;
    } else {
      value = null;
      return false;
    }
  }
}
