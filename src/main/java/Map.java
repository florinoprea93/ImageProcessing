package main.java;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;

import java.io.IOException;

/**
 * Created by florinoprea93 on 08.05.2017.
 */
public class Map extends Mapper<HipiImageHeader, FloatImage, Text, FloatImage> {

    private static final int FILTER_WIDTTH = 3;
    private static final int FILTER_HEIGHT = 3;
    private static final float MAX_PIXELS = 255.0f;


    private static  final double SIMPLE_EDGE_DETECTOR_FILTER[][] = {{-1.0f, -1.0f, -1.0f}, {-1.0f, 8.0f, -1.0f}, {-1.0f, -1.0f, -1.0f}};
    private static final double FACTOR = 1.0f;
    private static final double BIAS = 0.0f;


    public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException {


        if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {
            //TODO get dimensions of image
            int w = value.getWidth();
            int h = value.getHeight();

            //TODO get pointer to image data
            float[] valData = value.getData();
            float[] result = new float[valData.length];

            for (int i = 0; i < w; i++) {
                for (int j = 0; j < h; j++) {
                    double R = 0.0f;
                    double G = 0.0f;
                    double B = 0.0f;

                    for (int fJ = 0; fJ < FILTER_HEIGHT; fJ++) {
                        for (int fI = 0; fI < FILTER_WIDTTH; fI++) {

                            int imageI = (i - FILTER_WIDTTH / 2 + fI + w) % w;
                            int imageJ = (j - FILTER_HEIGHT / 2 + fJ + h) % h;

                            R += valData[(imageJ * w + imageI) * 3] * SIMPLE_EDGE_DETECTOR_FILTER[fJ][fI] * MAX_PIXELS;
                            G += valData[(imageJ * w + imageI) * 3 + 1] * SIMPLE_EDGE_DETECTOR_FILTER[fJ][fI] * MAX_PIXELS;
                            B += valData[(imageJ * w + imageI) * 3 + 2] * SIMPLE_EDGE_DETECTOR_FILTER[fJ][fI] * MAX_PIXELS;
                        }
                    }
                    result[(j * w + i) * 3] = Math.min(Math.max((float) (FACTOR * R + BIAS), 0.0f), MAX_PIXELS) / MAX_PIXELS;
                    result[(j * w + i) * 3 + 1] = Math.min(Math.max((float) (FACTOR * G + BIAS), 0.0f), MAX_PIXELS) / MAX_PIXELS;
                    result[(j * w + i) * 3 + 2] = Math.min(Math.max((float) (FACTOR * B + BIAS), 0.0f), MAX_PIXELS) / MAX_PIXELS;
                }
            }

            FloatImage floatImage = new FloatImage(w, h, value.getNumBands(), result);
            String filename = key.getAllMetaData().get("filename");

            //TODO emit record to reducer
            context.write(new Text(filename), floatImage);
        }
    }
}
