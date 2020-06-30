package Project1;

import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Query4 {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
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
                    String age;
                    String gender;
                    String ageGender;
                    while ((value = reader.readLine()) != null) {
                    	cacheData = value.split(",");
                        cId = cacheData[0];
                        age = cacheData[2];
                        gender = cacheData[3];
                        ageGender = age + "," + gender;
                        cIdMap.put(Integer.parseInt(cId), ageGender);
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
            String transTotal = transactions[2];
            String ageGender = cIdMap.get(cId);
            String ageGender_[] = ageGender.split(",");
            int age = Integer.parseInt(ageGender_[0]);
            String gender = ageGender_[1];
            String ageRange = "";
            if (age >= 10 && age < 20)
                ageRange = "10-19";
            if (age >= 20 && age < 30)
                ageRange = "20-29";
            if (age >= 30 && age < 40)
                ageRange = "30-39";
            if (age >= 40 && age < 50)
                ageRange = "40-49";
            if (age >= 50 && age < 60)
                ageRange = "50-59";
            if (age >= 60 && age < 71)
                ageRange = "60-70";
            context.write(new Text(ageRange + "," + gender),new Text(transTotal));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cCountryCode = "";
            double transTotal;
            double maxTransTotal = 0.0; 
            double minTransTotal = Double.MAX_VALUE;
            double transSum = 0.0;
            int transCount = 0;
            double transAvg = 0.0;
            for (Text val : values) {
                String groupValues = val.toString();
                transTotal = Float.parseFloat(groupValues);
                if (transTotal < minTransTotal) {
                    minTransTotal = transTotal;
                }
                if (transTotal > maxTransTotal) {
                    maxTransTotal = transTotal;
                }
                transSum += transTotal;
                transCount += 1;
                transAvg = transSum / transCount;
            }
            context.write(key, new Text(minTransTotal + "," + maxTransTotal + "," + transAvg)); 
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query4");
        job.setJarByClass(Query4.class);
        job.setMapperClass(Query4.Map.class);
        job.setReducerClass(Query4.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
