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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Query3 {
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
                    String cCountryCode;
                    while ((value = reader.readLine()) != null) {
                    	cacheData = value.split(",");
                        cId = cacheData[0];
                        cCountryCode = cacheData[4];
                        cIdMap.put(Integer.parseInt(cId), cCountryCode);
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
            double transTotal = Float.parseFloat(transactions[2]);
            int cCountryCode = Integer.parseInt(cIdMap.get(cId));
            context.write(new IntWritable(cCountryCode),new Text(cId + "," + transTotal));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cCountryCode = "";
            double transTotal;
            double maxTransTotal = 0.0;
            double minTransTotal = Double.MAX_VALUE;
            HashSet<Integer> cIdSet = new HashSet<>();
            for (Text val : values) {
                String[] groupValues = val.toString().split(",");
                cIdSet.add(Integer.parseInt(groupValues[0]));
                transTotal = Float.parseFloat(groupValues[1]);
                if (transTotal < minTransTotal) {
                    minTransTotal = transTotal;
                }
                if (transTotal > maxTransTotal) {
                    maxTransTotal = transTotal;
                }
            }
            context.write(key, new Text(cIdSet.size() + "," + minTransTotal + "," + maxTransTotal)); 
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);
        job.setMapperClass(Query3.Map.class);
        job.setReducerClass(Query3.Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
