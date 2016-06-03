package com.thu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jagesh on 5/10/16.
 */
public class Clustering
{

    public static class ClusteringM extends Mapper<CentroidWritable,PointsWritable,CentroidWritable,PointsWritable>
    {

        public List<Float[]> cent = new ArrayList<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            Path path =new Path("centroid/centroid");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line=br.readLine()) != null)
            {
                
		String[] strSplit = line.substring(1,line.length()-1).split(",");
                Float[] floatstr = new Float[strSplit.length];
                for (int i=0;i<strSplit.length; i++)
                {
                    floatstr[i] = Float.valueOf(strSplit[i]);
                }
                cent.add(floatstr);
            }
            br.close();
        }

        @Override
        protected void map(CentroidWritable key, PointsWritable value, Context context) throws IOException, InterruptedException
        {
            CentroidWritable centroid = new CentroidWritable(cent, value);

            context.write(centroid,value);
        }
    }
    public static class ClusteringR extends Reducer<CentroidWritable,PointsWritable,CentroidWritable,PointsWritable>
    {
        List<Float[]> centroidLst = new ArrayList<>();
        @Override
        protected void reduce(CentroidWritable key, Iterable<PointsWritable> values, Context context) throws IOException, InterruptedException
        {
            Float[] meanCentroid = new Float[41];
            Arrays.setAll(meanCentroid, index -> 0.0f);

            List<PointsWritable> pObjlst = new ArrayList<>();

            int j=0;
            for (PointsWritable value : values)
            {
                for (int i=0; i<41; i++)
                {
                    meanCentroid[i] += value.points[i];
                }
                j++;
                context.write(key, value);
            }

            for (int i=0;i<41;i++)
            {
                meanCentroid[i] = meanCentroid[i]/j;
            }


            centroidLst.add(meanCentroid);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
//            for (Float[] val : centroidLst)
//            {
//                System.out.println(Arrays.toString(val));
                Path path = new Path("centroid/centroid");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
                br.write(Arrays.toString(centroidLst.get(0)) + "\n");
                br.write(Arrays.toString(centroidLst.get(1)));
                br.close();
//            }
        }
    }

    public static void runClustering(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        FileSystem.getLocal(conf).delete(new Path("/user/2015280258/output_3/"), true);		//args[3]
        Job job = Job.getInstance(conf,"Clustering Job");

        job.setMapperClass(ClusteringM.class);
        job.setReducerClass(ClusteringR.class);

        job.setMapOutputKeyClass(CentroidWritable.class);
        job.setMapOutputValueClass(PointsWritable.class);
        job.setOutputKeyClass(CentroidWritable.class);
        job.setOutputValueClass(PointsWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/user/2015280258/output_2/"));		// args[2]
        FileOutputFormat.setOutputPath(job, new Path("/user/2015280258/output_3/"));		//args[3]

        job.waitForCompletion(true);
    }
}
