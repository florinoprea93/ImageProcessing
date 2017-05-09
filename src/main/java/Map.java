package main.java;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;

import java.io.IOException;

/**
 * Created by florinoprea93 on 08.05.2017.
 */
public class Map extends Mapper<HipiImageHeader, FloatImage, IntWritable, FloatImage> {
    public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException {


        //TODO verify that image was properly decoded, is of sufficient size, and has the three color channels(RGB)

        if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {
            //TODO get dimensions of image
            int w = value.getWidth();
            int h = value.getHeight();

            //TODO get pointer to image data
            float[] valData = value.getData();

            //TODO initialize 3 element array to hold RGB pixel average
            float[] avgData = {0, 0, 0};

            //TODO Traverse image pixel data in raster-scan order and update running average
            for(int j=0;j<h; j++) {
                for(int i=0; i<w; i++) {
                    avgData[0] += valData[(j*w+i)*3+0]; //TODO R
                    avgData[1] += valData[(j*w+i)*3+1]; //TODO G
                    avgData[2] += valData[(j*w+i)*3+2]; //TODO B
                }
            }

            //TODO Create a FloatImage to store average value
            FloatImage avg = new FloatImage(1, 1, 3, avgData);

            //TODO divide by number of pixels in image
            avg.scale(1.0f/(float) (w*h));

            //TODO emit record to reducer
            context.write(new IntWritable(1), avg);
        }
    }
}
