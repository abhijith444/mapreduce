import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by a575666 on 3/22/17.
 */
public class MaxDelay {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"MaxDelayMR");

        job.setJarByClass(MaxDelay.class);
        

        job.setMapperClass(DelayMapper.class);
        job.setMapOutputKeyClass(FlightKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setGroupingComparatorClass(DelayComparator.class);
        job.setPartitionerClass(DelayPartitioner.class);

        job.setReducerClass(DelayReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path[] inPaths = {new Path(args[0]),new Path(args[1])};
        Path outPath = new Path(args[2]);

        FileInputFormat.setInputPaths(job,inPaths);
        FileOutputFormat.setOutputPath(job,outPath);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class FlightKey implements WritableComparable<FlightKey>{
        Text arrivalAirport, flightNumber,year,month,day;

        public FlightKey() {
            this.arrivalAirport = new Text();
            this.flightNumber = new Text();
            this.year = new Text();
            this.month = new Text();
            this.day = new Text();
        }

        public FlightKey(Text arrivalAirport, Text flightNumber, Text year, Text month, Text day) {
            this.arrivalAirport = arrivalAirport;
            this.flightNumber = flightNumber;
            this.year = year;
            this.month = month;
            this.day = day;
        }

        public int compareTo(FlightKey o) {
            return this.arrivalAirport.toString().compareTo(o.arrivalAirport.toString());

        }

        public void write(DataOutput out) throws IOException {
            arrivalAirport.write(out);
            flightNumber.write(out);
            year.write(out);
            month.write(out);
            day.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            arrivalAirport.readFields(in);
            flightNumber.readFields(in);
            year.readFields(in);
            month.readFields(in);
            day.readFields(in);

        }

        public Text getArrivalAirport() {
            return arrivalAirport;
        }

        public void setArrivalAirport(Text arrivalAirport) {
            this.arrivalAirport = arrivalAirport;
        }

        public Text getFlightNumber() {
            return flightNumber;
        }

        public void setFlightNumber(Text flightNumber) {
            this.flightNumber = flightNumber;
        }

        public Text getYear() {
            return year;
        }

        public void setYear(Text year) {
            this.year = year;
        }

        public Text getMonth() {
            return month;
        }

        public void setMonth(Text month) {
            this.month = month;
        }

        public Text getDay() {
            return day;
        }

        public void setDay(Text day) {
            this.day = day;
        }

        @Override
        public String toString() {
            return "FlightKey{" +
                    "arrivalAirport=" + arrivalAirport +
                    ", flightNumber=" + flightNumber +
                    ", year=" + year +
                    ", month=" + month +
                    ", day=" + day +
                    '}';
        }
    }

    public static class DelayMapper extends Mapper<LongWritable, Text, FlightKey, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] flight = value.toString().split(",");
            FlightKey fk = new FlightKey(new Text(flight[0]), new Text(flight[2]), new Text(flight[3]), new Text(flight[4]),new Text(flight[5]));
            System.out.println(fk);
            try {
                IntWritable delay = new IntWritable(Integer.parseInt(flight[1]));
                context.write(fk, delay);
            }
            catch (Exception e){}
        }
    }

    public static class DelayReducer extends Reducer<FlightKey, IntWritable, Text, Text>{
        @Override
        protected void reduce(FlightKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> delayIt = values.iterator();
            ArrayList<Integer> delays = new ArrayList<Integer>();

            while (delayIt.hasNext()){
                delays.add(delayIt.next().get());
            }

            int maxDelay = Collections.max(delays);

            String outKey = String.format("%s,%s,%s", key.getArrivalAirport(),maxDelay,key.getFlightNumber());
            String outValue = String.format("%s,%s,%s",key.getYear(),key.getMonth(),key.getDay());

            context.write(new Text(outKey),new Text(outValue));
        }
    }

    public static class DelayPartitioner extends Partitioner<FlightKey, IntWritable>{
        @Override
        public int getPartition(FlightKey flightKey, IntWritable intWritable, int i) {
            if(flightKey.year.toString().equals("2007"))
                return 0;
            else
                return 1;


        }
    }

    public static class DelayComparator extends WritableComparator{

        protected DelayComparator(){
            super(FlightKey.class,true);
        }
        @Override
        public int compare(WritableComparable x, WritableComparable y) {
            FlightKey a = (FlightKey)x;
            FlightKey b = (FlightKey)y;

            return a.compareTo(b);
        }
    }
}
