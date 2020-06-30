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

public class Query2 {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        HashMap<Integer, String> cIdMap = null;
        public void setup(Context context) throws IOException, InterruptedException {
            cIdMap = new HashMap<>();
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
                    String salary;
                    String cNameSalary;
                    while ((value = reader.readLine()) != null) {
                    	cacheData = value.split(",");
                        cId = cacheData[0];
                        cName = cacheData[1];
                        salary = cacheData[5].toString();
                        cNameSalary = cName + "," + salary;
                        cIdMap.put(Integer.parseInt(cId), cNameSalary);
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
            String cNameSalary = cIdMap.get(cId);
            String[] cNameSalary_ = cNameSalary.split(",");
            String cName = cNameSalary_[0];
            String salary = cNameSalary_[1];
            int transCount = 1;
            double transTotal = Float.parseFloat(transactions[2]);
            int transItems = Integer.parseInt(transactions[3]);
            context.write(new IntWritable(cId),new Text(cName + "," + salary + "," + transCount + "," + transTotal + "," + transItems));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cName = "";
            double salary = 0.0;
            int count = 0;
            double transTotal = 0.0;
            int transItems;
            int minTransItems = 11;
            for (Text val : values) {
                String[] groupValues = val.toString().split(",");
                cName = groupValues[0];
                salary = Float.parseFloat(groupValues[1]);
                count += Integer.parseInt(groupValues[2]);
                transTotal += Float.parseFloat(groupValues[3]);
                transItems = Integer.parseInt(groupValues[4]);
                if (transItems < minTransItems) {
                    minTransItems = transItems;
                }
            }
            context.write(key, new Text(cName + "," + salary + "," + count + "," + transTotal + "," + minTransItems));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2.Map.class);
        //job.setCombinerClass(Query2.Reduce.class);
        job.setReducerClass(Query2.Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
