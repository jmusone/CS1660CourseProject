import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class iiTestTwo {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String file = ((FileSplit) context.getInputSplit()).getPath().getName();
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        if(shortList()){
          context.write(word, new Text(file));  
        }
      }
    }
    
    private boolean shortList(){
      if(word.equals(new Text("the")) || word.equals(new Text("a")) || word.equals(new Text("an")) || word.equals(new Text("that")) || word.equals(new Text("to")) || word.equals(new Text("be")) || word.equals(new Text("of")) || word.equals(new Text("and")) || word.equals(new Text("in")) || word.equals(new Text("it"))){
        return false;
      }     
      return true;      
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      //key is the word, while value is the filename
      HashMap<Text, IntWritable> indexTracker = new HashMap<Text, IntWritable>(); //this holds the file + number of instances for a word in that file
      int total = 0;
      for(Text file : values){
        if(indexTracker != null && indexTracker.get(file) != null){
          int wordAmount = indexTracker.get(file).get();
          indexTracker.put(file, new IntWritable(wordAmount + 1));
          total++;
        }
        else{
          indexTracker.put(file, new IntWritable(1));
          total++;
        }
      }
      context.write(key, new Text(indexTracker.toString()));        
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
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}