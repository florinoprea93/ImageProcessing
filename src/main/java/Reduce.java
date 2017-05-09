package main.java;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hipi.image.FloatImage;

import java.io.IOException;

/**
 * Created by florinoprea93 on 08.05.2017.
 */
public class Reduce extends Reducer<IntWritable, FloatImage, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<FloatImage> values, Context context) throws IOException, InterruptedException {

        //TODO create FloatImage object to hold final result
        FloatImage avg = new FloatImage(1, 1, 3);

        //TODO initialize a counter and iterate over IntWritable/FloatImage records from mapper
        int total = 0;
        for (FloatImage value: values) {
            avg.add(value);
            total++;
        }

        if(total > 0) {
            //TODO normalize sum to obtain average
            avg.scale(1.0f/total);

            //TODO assemble final output as string
            float[] avgData = avg.getData();
            String result = String.format("Average pixel value: %f %f %f", avgData[0], avgData[1], avgData[2]);

            //TODO emit output of job which wil be written to HDFS
            context.write(key, new Text(result));
        }
    }
}
