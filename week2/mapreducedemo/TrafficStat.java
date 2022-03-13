package week2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class TrafficStat {

    public static class TrafficMapper extends Mapper<Object, Text, Text, PhoneTraffic> {

        public void map(Object key, Text value, Context context) {
            String[] lines = value.toString().split("\t");
            if (lines.length < 10) {
                return;
            }
            String phone = lines[1];

            try {
                long up = Long.parseLong(lines[7]);
                long down = Long.parseLong(lines[8]);
                context.write(new Text(phone), new PhoneTraffic(up, down, up + down));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                System.out.println("parseLong failed " + e.getMessage());
            }
        }
    }

    public static class TrafficReducer extends Reducer<Object, Text, Text, PhoneTraffic> {

        public void reduce(Text key, Iterable<PhoneTraffic> values, Context context) {
            int totalUp = 0;
            int totalDown = 0;
            int sumTraffic = 0;

            for (PhoneTraffic val : values) {
                totalUp += val.getUp();
                totalDown += val.getDown();
                sumTraffic += val.getSum();
            }
            try {
                context.write(key, new PhoneTraffic(totalUp, totalDown, sumTraffic));
            } catch (IOException | InterruptedException e) {
                System.out.println("total parse long failed " + e.getMessage());
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: TrafficStat <in> <out>");
            System.exit(2);
        }
        System.err.println("otherArgs: " + Arrays.toString(otherArgs));

        Job job = Job.getInstance(conf, "TrafficStat");
        job.setJarByClass(TrafficStat.class);
        job.setMapperClass(TrafficStat.TrafficMapper.class);
        job.setCombinerClass(TrafficStat.TrafficReducer.class);
        job.setReducerClass(TrafficStat.TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneTraffic.class);

        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
