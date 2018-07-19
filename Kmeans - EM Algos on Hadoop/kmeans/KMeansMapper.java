package kmeans;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static kmeans.KMeansJob.DIMENSIONAL;
import static kmeans.KMeansJob.centers;

public class KMeansMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context contex) throws IOException, InterruptedException {
        String[] temp = value.toString().split(",");
        Double[] point = new Double[DIMENSIONAL];
        for (int i = 0; i < DIMENSIONAL; i++) {
            point[i] = Double.parseDouble(temp[i]);
        }

        //find the nearest center from a point
        double min = Double.MAX_VALUE;
        Double[] nearestCenter = centers.get(0);
        for (Double[] p : centers) {
            int d = 0;
            for (int i = 0; i < DIMENSIONAL; i++) {
                Double dif = point[i] - p[i];
                d += dif * dif;
            }
            if (d < min) {
                min = d;
                nearestCenter = p;
            }
        }
        String strCentr = "";
        for (int i = 0; i < nearestCenter.length; i++) {
            strCentr += "," + nearestCenter[i];
        }
        strCentr = strCentr.substring(1);
        //emit the nearest center and the point
        contex.write(new Text(strCentr), value);
    }
}