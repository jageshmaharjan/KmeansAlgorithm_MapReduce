package com.thu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Text;

/**
 * Created by jagesh on 5/9/16.
 */
public class RemoveDuplicates
{
    public static class DuplicateRemoveM extends Mapper<Object, Text, Text, NullWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            context.write(value,NullWritable.get());
        }
    }

    public static class DuplicateRemoveR extends Reducer<Text,NullWritable,Text,NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
        {
            context.write(key,NullWritable.get());
        }
    }

    public static void runDuplicateRemover(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        FileSystem.getLocal(conf).delete(new Path("/user/2015280258/output_1/"), true);
        Job job = Job.getInstance(conf,"Remove Duplicate");

        job.setJarByClass(RemoveDuplicates.class);
        job.setMapperClass(DuplicateRemoveM.class);
        job.setReducerClass(DuplicateRemoveR.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/user/2015280258/input/kddcup.testdata.unlabeled"));	//args[0]
        FileOutputFormat.setOutputPath(job,new Path("/user/2015280258/output_1/"));		//args[1] 

        job.waitForCompletion(true);

    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException
    {
        runDuplicateRemover(args);
        ParseNormalize.runParserAndNormalizer(args);
        Clustering.runClustering(args);
    }
}
