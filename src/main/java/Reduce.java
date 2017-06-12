package main.java;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hipi.image.FloatImage;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * Created by florinoprea93 on 08.05.2017.
 */
public class Reduce extends Reducer<Text, FloatImage, Text, FloatImage> {
    public void reduce(Text key, Iterable<FloatImage> values, Context context) throws IOException, InterruptedException {

        for (FloatImage val : values) {
            String filename = "output" + key.toString();
            try {
                float[] data = val.getData();
                int w = val.getWidth();
                int h = val.getHeight();
                BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
                for (int y = 0; y < h; y++) {
                    for (int x = 0; x < w; x++) {
                        Color color = new Color((float) data[(y * w + x) * 3],
                                (float) data[(y * w + x) * 3 + 1],
                                (float) data[(y * w + x) * 3 + 2]);
                        bi.setRGB(x, y, color.getRGB());
                    }
                }

                ImageIO.write(bi, "png", new File(filename));
                context.write(new Text(filename), val);

            } catch (IOException e) {
                System.out.println("---->REDUCE ERROR<----");
                e.printStackTrace();
            }
        }
    }
}
