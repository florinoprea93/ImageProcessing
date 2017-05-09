package main.java;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hipi.image.FloatImage;
import org.hipi.imagebundle.mapreduce.HibInputFormat;

/**
 * Created by florinoprea93 on 08.05.2017.
 */
public class Main extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        //TODO check input arguments
        if (args.length != 2) {
            System.out.println("Usage : Main <input HIB> <output directory>");
            System.exit(0);
        }

        //TODO initialize and configure MapReduce job
        Job job = Job.getInstance();

        //TODO set input format class which parses the input HIB and spawns map tasks
        job.setInputFormatClass(HibInputFormat.class);

        //TODO set the driver, mapper and reduces classes which express the computation
        job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //TODO set the types for the key/value pairs to/from map and reduce layers
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatImage.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //TODO set the input and output paths on the HDFS
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //TODO execute the MapReduce job and block until it complets
        boolean success = job.waitForCompletion(true);

        //TODO return success or failure
        return success ? 0 : 1;

    }

    public static void  main(String[] args) throws Exception {
        ToolRunner.run(new Main(), args);
        System.exit(0);
    }
}
