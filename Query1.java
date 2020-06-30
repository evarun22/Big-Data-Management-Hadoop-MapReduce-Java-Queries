package Project1;

import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Query1 {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        HashMap<Integer, String> cIdName = null;
        public void setup(Context context) throws IOException, InterruptedException {
            cIdName = new HashMap<>();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                    String value;
                    String[] cacheData;
                    String cId;
                    String cName;
                    while ((value = reader.readLine()) != null) {
                    	cacheData = value.split(",");
                        cId = cacheData[0];
                        cName = cacheData[1];
                        cIdName.put(Integer.parseInt(cId), cName);
                    }
            	} catch (Exception ex) {
                    System.out.println("Unable to read the file");
                    System.exit(1);
                }
	    }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] transactions = value.toString().split(",");
            int cId = Integer.parseInt(transactions[1]);
            String cName = cIdName.get(cId);
            int transCount = 1;
            double transTotal = Float.parseFloat(transactions[2]);
            context.write(new IntWritable(cId),new Text(cName + "," + transCount + "," + transTotal));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cName = "";
            int count = 0;
            double sum = 0.0;
            for (Text val : values) {
                String[] groupValues = val.toString().split(",");
                cName = groupValues[0];
                count += Integer.parseInt(groupValues[1]);
                sum += Float.parseFloat(groupValues[2]);
            }
            context.write(key, new Text(cName + "," + count + "," + sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);
        job.setMapperClass(Query1.Map.class);
        job.setCombinerClass(Query1.Reduce.class);
        job.setReducerClass(Query1.Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
