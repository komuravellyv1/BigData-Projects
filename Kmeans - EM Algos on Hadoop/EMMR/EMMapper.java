package EMMR;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

import static EMMR.EMJob.DIMENSIONAL;
import static EMMR.EMJob.centers;

public class EMMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] temp = value.toString().split(",");
        Double[] point = new Double[DIMENSIONAL];
        for (int i = 0; i < DIMENSIONAL; i++) {
            point[i] = Double.parseDouble(temp[i]);
        }

        //find the nearest center from a point
        double min = Double.MAX_VALUE;
        Double[] nearestCenter = centers.keySet().stream().findAny().get();
        for (Map.Entry<Double[], Integer> entry : centers.entrySet()) {
            if (entry.getValue() <= 0) continue;
            int d = 0;
            for (int i = 0; i < DIMENSIONAL; i++) {
                Double dif = point[i] - entry.getKey()[i];
                d += dif * dif * entry.getValue() / 150;
            }
            if (d < min) {
                min = d;
                nearestCenter = entry.getKey();
            }
        }
        centers.put(nearestCenter, centers.getOrDefault(nearestCenter, 1) - 1);
        String strCentr = "";
        if (nearestCenter != null)
            for (int i = 0; i < nearestCenter.length; i++) {
                strCentr += "," + nearestCenter[i];
            }
        if (strCentr.length() > 1)
            strCentr = strCentr.substring(1);
        //emit the nearest center and the point
        context.write(new Text(strCentr), value);
    }
}
