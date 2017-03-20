import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by a575666 on 3/19/17.
 */
public class WordCount {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("AbhiMRWordCount");

        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(myMapper.class);
        conf.setCombinerClass(myReducer.class);
        conf.setReducerClass(myReducer.class);
        conf.setPartitionerClass(HashPartitioner.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);


        JobClient.runJob(conf);

    }

    static class myMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            String line = value.toString();

            for(String word:line.split(" ")){
                if (word.length()>0){
                    outputCollector.collect(new Text(word),new LongWritable(1));
                }
            }

        }
    }

    static class myReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{


        public void reduce(Text word, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            int sum=0;
            while (values.hasNext()){
                sum += values.next().get();
            }
            outputCollector.collect(word,new LongWritable(sum));
        }
    }

    static class myKey implements WritableComparable<myKey>{

        public int compareTo(myKey o) {
            return 0;
        }

        public void write(DataOutput dataOutput) throws IOException {

        }

        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    static class myValue implements Writable {

        public void write(DataOutput dataOutput) throws IOException {

        }

        public void readFields(DataInput dataInput) throws IOException {

        }
    }
}
