import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GiaMaxMin {

    public static class MaxMinMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text tenMatHang = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Bỏ qua dòng tiêu đề
            if (value.toString().startsWith("ten,gia,donvitinh,ngaycapnhat")) return;

            // Tách theo dấu phẩy, loại bỏ dấu ngoặc kép nếu có
            String[] fields = value.toString().split("\",\"");
            if (fields.length >= 4) {
                try {
                    String ten = fields[0].replaceAll("^\"|\"$", "").trim();
                    String giaStr = fields[1].replaceAll("^\"|\"$", "").trim();
                    int gia = (int) Double.parseDouble(giaStr);

                    tenMatHang.set(ten);
                    context.write(tenMatHang, new IntWritable(gia));
                } catch (NumberFormatException ignored) {
                }
            }
        }
    }

    public static class MaxMinReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;

            for (IntWritable val : values) {
                int gia = val.get();
                if (gia < min) min = gia;
                if (gia > max) max = gia;
            }

            context.write(key, new Text("Min: " + min + " - Max: " + max));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gia cao nhat thap nhat");

        job.setJarByClass(GiaMaxMin.class);
        job.setMapperClass(MaxMinMapper.class);
        job.setReducerClass(MaxMinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}