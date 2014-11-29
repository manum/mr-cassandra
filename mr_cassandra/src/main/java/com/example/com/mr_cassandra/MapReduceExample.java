/*
 *  Author: Manu Mukerji <next2manu@gmail.com>
 * 
 */

package com.example.com.mr_cassandra;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/*
 *  Author: Manu Mukerji <next2manu@gmail.com>
 * 
 */

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduceExample {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private CassandraHelper cclient = new CassandraHelper();


        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(value, one);
            cclient.addKey(line.toString());
        }

        public void setup(Context context) {
            cclient.createConnection(""); 
        }

        public void cleanup(Context context) {
            cclient.closeConnection();
        }

    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MR Keying");
        job.setJarByClass(MapReduceExample.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
