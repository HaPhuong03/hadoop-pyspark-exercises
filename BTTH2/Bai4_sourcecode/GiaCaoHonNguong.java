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

public class GiaCaoHonNguong {

    public static class GiaMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text ngayCapNhat = new Text();
        private final static IntWritable one = new IntWritable(1);
        private static double nguongGia;

        @Override
        protected void setup(Context context) {
            // Đọc ngưỡng giá từ cấu hình
            nguongGia = context.getConfiguration().getDouble("nguongGia", 0);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Bỏ dòng tiêu đề
            if (value.toString().toLowerCase().contains("gia") && value.toString().toLowerCase().contains("ngaycapnhat")) {
                return;
            }

            String[] parts = value.toString().split(",");

            // Gom các phần tử nếu tên sản phẩm có dấu phẩy
            if (parts.length > 4) {
                // Ghép lại tên (có thể chứa dấu phẩy)
                String ten = parts[0];
                for (int i = 1; i <= parts.length - 4; i++) {
                    ten += "," + parts[i];
                }
                parts = new String[]{ten, parts[parts.length - 3], parts[parts.length - 2], parts[parts.length - 1]};
            }

            if (parts.length == 4) {
                // Loại bỏ dấu ngoặc kép nếu có
                for (int i = 0; i < parts.length; i++) {
                    parts[i] = parts[i].replace("\"", "").trim();
                }

                try {
                    double gia = Double.parseDouble(parts[1]);
                    if (gia > nguongGia) {
                        ngayCapNhat.set(parts[3]);  // ngày cập nhật
                        context.write(ngayCapNhat, one);
                    }
                } catch (NumberFormatException e) {
                    // Bỏ qua dòng lỗi
                }
            }
        }
    }

    public static class GiaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: GiaCaoHonNguong <input path> <output path> <nguongGia>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setDouble("nguongGia", Double.parseDouble(args[2])); // Đọc ngưỡng từ dòng lệnh

        Job job = Job.getInstance(conf, "Thong ke gia cao hon nguong");
        job.setJarByClass(GiaCaoHonNguong.class);
        job.setMapperClass(GiaMapper.class);
        job.setReducerClass(GiaReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));     // input
        FileOutputFormat.setOutputPath(job, new Path(args[1]));   // output

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}