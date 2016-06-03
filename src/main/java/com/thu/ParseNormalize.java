package com.thu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by jagesh on 5/9/16.
 */
public class ParseNormalize
{
    public static class ParseNormalizeM extends Mapper<Object,Text,CentroidWritable,PointsWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            PointsWritable points = new PointsWritable(value.toString());
            CentroidWritable centroid = new CentroidWritable();

            context.write(centroid,points);
        }
    }

    public static class ParseNormalizeR extends Reducer<CentroidWritable,PointsWritable,CentroidWritable,PointsWritable>
    {
        @Override
        protected void reduce(CentroidWritable key, Iterable<PointsWritable> values, Context context) throws IOException, InterruptedException
        {
            for (PointsWritable value : values)
            {
                context.write(key,value);
            }
        }
    }

    public static void runParserAndNormalizer(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        FileSystem.getLocal(conf).delete(new Path("/user/2015280258/output_2/"), true);	//args[2]
        Job job = Job.getInstance(conf,"Parse And Normalization Job");

        job.setMapperClass(ParseNormalizeM.class);
        job.setReducerClass(ParseNormalizeR.class);

        job.setMapOutputKeyClass(CentroidWritable.class);
        job.setMapOutputValueClass(PointsWritable.class);
        job.setOutputKeyClass(CentroidWritable.class);
        job.setOutputValueClass(PointsWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/user/2015280258/output_1/"));	//args[1]
        FileOutputFormat.setOutputPath(job, new Path("/user/2015280258/output_2/"));		// args[2]

        job.waitForCompletion(true);

    }
}
