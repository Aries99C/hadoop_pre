package reducer;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LinearReducer extends Reducer<Text, Text, Text, Text> {

    private final List<Double> yList = new ArrayList<>();
    private final List<Double[]> xList = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] attributes = value.toString().split("\\|");
            double longitude = Double.parseDouble(attributes[1]);
            double latitude = Double.parseDouble(attributes[2]);
            double altitude = Double.parseDouble(attributes[3]);
            double income = Double.parseDouble(attributes[11]);
            double rating = Double.parseDouble(attributes[6]);
            Double[] xValues = new Double[4];
            xValues[0] = longitude;
            xValues[1] = latitude;
            xValues[2] = altitude;
            xValues[3] = income;
            xList.add(xValues);
            yList.add(rating);
//            String text = attributes[1] + "," + attributes[2] + "," + attributes[3] + "," + attributes[11] + ": " + attributes[6];
//            context.write(new Text(text), new Text(""));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double[] y = new double[yList.size()];
        int count = 0;
        for (double value : yList) {
            y[count++] = value;
        }
        double[][] x = new double[xList.size()][4];
        count = 0;
        for (Double[] values : xList) {
            for (int i = 0; i < 4; i++) {
                x[count][i] = values[i];
            }
            count++;
        }
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.newSampleData(y, x);
        StringBuilder builder = new StringBuilder();
        for (double theta : regression.estimateRegressionParameters()) {
            builder.append(theta).append(",");
        }
        context.write(new Text(builder.toString()), new Text(""));
    }
}
