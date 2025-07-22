import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GiaTrungBinh {

    public static class TrungBinhMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text tenMatHang = new Text();
        private DoubleWritable giaWritable = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Bỏ qua dòng tiêu đề
            if (line.toLowerCase().contains("gia") && line.toLowerCase().contains("ngaycapnhat")) {
                return;
            }

            String[] parts = line.split(",");

            // Ghép lại tên mặt hàng nếu chứa dấu phẩy
            if (parts.length > 4) {
                StringBuilder ten = new StringBuilder(parts[0]);
                for (int i = 1; i <= parts.length - 4; i++) {
                    ten.append(",").append(parts[i]);
                }
                parts = new String[]{
                        ten.toString(), // ten
                        parts[parts.length - 3], // gia
                        parts[parts.length - 2], // donvitinh
                        parts[parts.length - 1]  // ngaycapnhat
                };
            }

            if (parts.length == 4) {
                try {
                    // Xử lý dữ liệu sạch
                    String ten = parts[0].replace("\"", "").trim();
                    double gia = Double.parseDouble(parts[1].replace("\"", "").trim());

                    tenMatHang.set(ten);
                    giaWritable.set(gia);
                    context.write(tenMatHang, giaWritable);
                } catch (NumberFormatException ignored) {
                }
            }
        }
    }

    public static class TrungBinhReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                context.write(key, new DoubleWritable(sum / count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GiaTrungBinh <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gia trung binh");

        job.setJarByClass(GiaTrungBinh.class);
        job.setMapperClass(TrungBinhMapper.class);
        job.setReducerClass(TrungBinhReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}