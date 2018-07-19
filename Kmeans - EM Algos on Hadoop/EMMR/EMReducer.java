package EMMR;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import static EMMR.EMJob.DIMENSIONAL;

public class EMReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double[] newCenter = new double[DIMENSIONAL];
        int size = 0;
        String points = "";
        Iterator<Text> valuesIterator = values.iterator();
        while (valuesIterator.hasNext()) {
            String curPoint = valuesIterator.next().toString();
            String[] next = curPoint.split(",");
            for (int i = 0; i < DIMENSIONAL; i++) {
                newCenter[i] += Double.parseDouble(next[i]);
            }
            points += "[";
            points += curPoint;
            points += "]";
            size++;
        }

        for (int i = 0; i < DIMENSIONAL; i++) {
            newCenter[i] = newCenter[i] / size;
        }
        String strCentr = "";
        for (int i = 0; i < newCenter.length; i++) {
            strCentr += "," + newCenter[i];
        }
        strCentr = strCentr.substring(1);
        //emit new center and point
        context.write(new Text(strCentr), new Text(points));
    }
}
