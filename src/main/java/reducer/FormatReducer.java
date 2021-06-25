package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

public class FormatReducer extends Reducer<Text, Text, Text, Text> {
    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    SimpleDateFormat format2 = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    SimpleDateFormat format3 = new SimpleDateFormat("MMMM dd,yyyy", Locale.ENGLISH);
    String regex1 = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
    Pattern pattern1 = Pattern.compile(regex1);
    String regex2 = "[0-9]{4}/[0-9]{2}/[0-9]{2}";
    Pattern pattern2 = Pattern.compile(regex2);
    String regex3 = "[a-zA-Z]+\\s[0-9]+,[0-9]{4}";
    Pattern pattern3 = Pattern.compile(regex3);
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String line = value.toString();
            String[] words = line.split("\\|");
            String reviewDate = words[4];
            String birthday = words[8];
            try {
                if (pattern1.matcher(reviewDate).matches()) {

                }
                if (pattern2.matcher(reviewDate).matches()) {
                    reviewDate = format1.format(format2.parse(reviewDate));
                }
                if (pattern3.matcher(reviewDate).matches()) {
                    reviewDate = format1.format(format3.parse(reviewDate));
                }
                if (pattern1.matcher(birthday).matches()) {

                }
                if (pattern2.matcher(birthday).matches()) {
                    birthday = format1.format(format2.parse(birthday));
                }
                if (pattern3.matcher(birthday).matches()) {
                    birthday = format1.format(format3.parse(birthday));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            line = line.replace(words[4], reviewDate);
            line = line.replace(words[8], birthday);

            String temperature = words[5];
            double temperatureValue = Double.parseDouble(temperature.substring(0, temperature.length()-1));
            String temperatureType = temperature.substring(temperature.length()-1);
            if (temperatureType.equals("℉")) {
                temperatureValue = (temperatureValue - 32) / 1.8;
                temperature = temperatureValue + "℃";
            }
            line = line.replace(words[5], temperature);

            context.write(new Text(line), new Text(""));
        }
    }
}
