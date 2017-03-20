import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomDriverTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CustomKeyValueTest");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);
        job.setMapOutputKeyClass(Country.class);
        job.setMapOutputValueClass(Person.class);
        job.setOutputKeyClass(Country.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    static class myMapper extends Mapper<LongWritable, Text, Country, Person>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            System.out.println("Processing ... "+record[1]);
            context.write(new Country(record[0]),new Person(record[1]));
        }
    }

    static class myReducer extends Reducer<Country, Person, Country, Text>{

        @Override
        protected void reduce(Country country, Iterable<Person> persons, Context context) throws IOException, InterruptedException {
            String out="(";
            for(Person p:persons )
                out +=p.toString()+", ";
            out+=")";

            context.write(country,new Text(out));
        }
    }

    static class Country implements WritableComparable<Country> {

        private Text name;

        public Text getName() {
            return name;
        }

        public void setName(Text name) {
            this.name = name;
        }

        public Country() {
            name = new Text();
        }

        public Country(String name) {
            this.name = new Text(name);
        }

        @Override
        public String toString() {
            return name.toString();
        }

        public int compareTo(Country o) {
            return o.toString().compareTo(this.toString());
        }

        public void write(DataOutput out) throws IOException {
            name.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            name.readFields(in);
        }
    }

    static class Person implements WritableComparable<Person> {

        private Text name;

        public Person() {
            name = new Text();
        }

        public Person(String name) {
            this.name = new Text(name);
        }

        public Text getName() {
            return name;
        }

        public void setName(Text name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name.toString();
        }

        public int compareTo(Person o) {
            return this.toString().compareTo(o.toString());
        }

        public void write(DataOutput out) throws IOException {
            name.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            name.readFields(in);
        }


    }
}
