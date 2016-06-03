package com.thu;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jagesh on 5/9/16.
 */
public class CentroidWritable implements WritableComparable<CentroidWritable>
{
    public int cols = 41;
    public Float[] centroid = new Float[cols];

    public CentroidWritable()
    {
        this.centroid = new Float[cols];
        Arrays.setAll(centroid, index -> 0.0f);

     }

    public CentroidWritable(Float[] c)
    {
        this.centroid = c;
    }

    public CentroidWritable(List<Float[]> cent, PointsWritable value)
    {
        float distance1 = calculateDistance(cent.get(0),value.points);
        float distance2 = calculateDistance(cent.get(1),value.points);
        if (distance1 < distance2)
            centroid = cent.get(0);
        else
            centroid = cent.get(1);
    }

    private float calculateDistance(Float[] c, Float[] p)
    {
        float sum =0;
        for (int i=0;i<p.length; i++)
        {
            sum += Math.pow((p[i] - c[i]) ,2);
        }

        return (float) Math.sqrt(sum);
    }


    @Override
    public String toString() {
        return "CentroidWritable{" +
                "cols=" + cols +
                ", centroid=" + Arrays.toString(centroid) +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }


    @Override
    public int compareTo(CentroidWritable o)
    {
        return  (Arrays.equals(o.centroid,this.centroid) ? 0 : -1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        for (int i=0;i<cols;i++)
        {
            dataOutput.writeFloat(centroid[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        for (int i=0; i<cols;i++)
        {
            centroid[i] = dataInput.readFloat();
        }
    }
}
