package com.thu;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jagesh on 5/9/16.
 */
public class PointsWritable implements WritableComparable<PointsWritable>
{
    public int cols = 41;
    public Float[] points = new Float[cols];
    public float magnitude = 0;

    public PointsWritable()
    {
        this.points = new Float[cols];
        this.magnitude = 0;

    }

    public PointsWritable(String s)
    {
        float sum = 0;
        String[] strsplit = s.split(",");
        for (int i=0; i< strsplit.length; i++)
        {
            if (i == 1)
                points[i] = mapFirstCol.get(strsplit[i]);
            else if (i == 2)
                points[i] = mapSecondCol.get(strsplit[i]);
            else if (i == 3)
                points[i] = mapThirdCol.get(strsplit[i]);
            else
                points[i] = Float.valueOf(strsplit[i]);

            sum += Math.pow(points[i],2);
        }

        magnitude = (float) Math.sqrt(sum);
        for (int i =0; i< points.length; i++)
        {
            points[i] = points[i]/magnitude;
        }
    }

    public PointsWritable(PointsWritable value)
    {
        System.arraycopy(value.points,0,points,0,cols);
        this.magnitude = value.magnitude;
    }

    public PointsWritable(Float[] floats, Float magn)
    {
        System.arraycopy(floats,0,points,0,cols);
        for (int i=0;i<floats.length; i++)
        {
            points[i] = floats[i];
        }
        magnitude = magn;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return "Point: "+
                Arrays.toString(points) +
                "Magnitude:" +
                magnitude;
    }

    public static Map<String,Float> mapFirstCol = new HashMap<>();
    public static Map<String,Float> mapSecondCol = new HashMap<>();
    public static Map<String,Float> mapThirdCol = new HashMap<>();
    static
    {
        mapFirstCol.put("icmp", Float.valueOf(1));
        mapFirstCol.put("tcp", Float.valueOf(2));
        mapFirstCol.put("udp", Float.valueOf(3));
        mapSecondCol.put("aol", Float.valueOf(1));
        mapSecondCol.put("auth", Float.valueOf(2));
        mapSecondCol.put("bgp", Float.valueOf(3));
        mapSecondCol.put("courier", Float.valueOf(4));
        mapSecondCol.put("csnet_ns", Float.valueOf(5));
        mapSecondCol.put("ctf", Float.valueOf(6));
        mapSecondCol.put("daytime", Float.valueOf(7));
        mapSecondCol.put("discard",Float.valueOf(8));
        mapSecondCol.put("domain",Float.valueOf(9));
        mapSecondCol.put("domain_u",Float.valueOf(10));
        mapSecondCol.put("echo",Float.valueOf(11));
        mapSecondCol.put("eco_i",Float.valueOf(12));
        mapSecondCol.put("ecr_i",Float.valueOf(13));
        mapSecondCol.put("efs",Float.valueOf(14));
        mapSecondCol.put("exec",Float.valueOf(15));
        mapSecondCol.put("finger",Float.valueOf(16));
        mapSecondCol.put("ftp",Float.valueOf(17));
        mapSecondCol.put("ftp_data",Float.valueOf(18));
        mapSecondCol.put("gopher",Float.valueOf(19));
        mapSecondCol.put("harvest",Float.valueOf(20));
        mapSecondCol.put("hostnames",Float.valueOf(21));
        mapSecondCol.put("http",Float.valueOf(22));
        mapSecondCol.put("http_2784",Float.valueOf(23));
        mapSecondCol.put("http_443",Float.valueOf(24));
        mapSecondCol.put("http_8001",Float.valueOf(25));
        mapSecondCol.put("imap4",Float.valueOf(26));
        mapSecondCol.put("IRC",Float.valueOf(27));
        mapSecondCol.put("iso_tsap",Float.valueOf(28));
        mapSecondCol.put("klogin",Float.valueOf(29));
        mapSecondCol.put("kshell",Float.valueOf(30));
        mapSecondCol.put("ldap",Float.valueOf(31));
        mapSecondCol.put("link",Float.valueOf(32));
        mapSecondCol.put("login",Float.valueOf(33));
        mapSecondCol.put("mtp",Float.valueOf(34));
        mapSecondCol.put("name",Float.valueOf(35));
        mapSecondCol.put("netbios_dgm",Float.valueOf(36));
        mapSecondCol.put("netbios_ns",Float.valueOf(37));
        mapSecondCol.put("netbios_ssn",Float.valueOf(38));
        mapSecondCol.put("netstat",Float.valueOf(39));
        mapSecondCol.put("nnsp",Float.valueOf(40));
        mapSecondCol.put("nntp",Float.valueOf(41));
        mapSecondCol.put("ntp_u",Float.valueOf(42));
        mapSecondCol.put("other",Float.valueOf(43));
        mapSecondCol.put("pm_dump",Float.valueOf(44));
        mapSecondCol.put("pop_2",Float.valueOf(45));
        mapSecondCol.put("pop_3",Float.valueOf(46));
        mapSecondCol.put("printer",Float.valueOf(47));
        mapSecondCol.put("private",Float.valueOf(48));
        mapSecondCol.put("remote_job",Float.valueOf(49));
        mapSecondCol.put("rje",Float.valueOf(50));
        mapSecondCol.put("shell",Float.valueOf(51));
        mapSecondCol.put("smtp",Float.valueOf(52));
        mapSecondCol.put("sql_net",Float.valueOf(53));
        mapSecondCol.put("ssh",Float.valueOf(54));
        mapSecondCol.put("sunrpc",Float.valueOf(55));
        mapSecondCol.put("supdup",Float.valueOf(56));
        mapSecondCol.put("systat",Float.valueOf(57));
        mapSecondCol.put("telnet",Float.valueOf(58));
        mapSecondCol.put("tftp_u",Float.valueOf(59));
        mapSecondCol.put("tim_i",Float.valueOf(60));
        mapSecondCol.put("time",Float.valueOf(61));
        mapSecondCol.put("urp_i",Float.valueOf(62));
        mapSecondCol.put("uucp",Float.valueOf(63));
        mapSecondCol.put("uucp_path",Float.valueOf(64));
        mapSecondCol.put("vmnet",Float.valueOf(65));
        mapSecondCol.put("whois",Float.valueOf(66));
        mapSecondCol.put("X11",Float.valueOf(67));
        mapSecondCol.put("Z39_50",Float.valueOf(68));
        mapSecondCol.put("urh_i",Float.valueOf(69));
        mapSecondCol.put("icmp",Float.valueOf(70));
        mapThirdCol.put("OTH",Float.valueOf(1));
        mapThirdCol.put("REJ",Float.valueOf(2));
        mapThirdCol.put("RSTO",Float.valueOf(3));
        mapThirdCol.put("RSTOS0",Float.valueOf(4));
        mapThirdCol.put("RSTR",Float.valueOf(5));
        mapThirdCol.put("S0",Float.valueOf(6));
        mapThirdCol.put("S1",Float.valueOf(7));
        mapThirdCol.put("S2",Float.valueOf(8));
        mapThirdCol.put("S3",Float.valueOf(9));
        mapThirdCol.put("SF",Float.valueOf(10));
        mapThirdCol.put("SH",Float.valueOf(11));
    }

    @Override
    public int compareTo(PointsWritable o)
    {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeFloat(magnitude);
        for (int i=0;i<cols;i++)
        {
            dataOutput.writeFloat(points[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        magnitude = dataInput.readFloat();
        for (int i=0; i<cols; i++)
        {
            points[i] = dataInput.readFloat();
        }
    }
}
