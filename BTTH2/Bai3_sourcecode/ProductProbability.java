import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProductProbability {

    // Mapper Class
    public static class ProductMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text productPair = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Tách các sản phẩm trong giao dịch
            String line = value.toString();
            String[] products = line.split("\\s+");

            // Đếm các cặp sản phẩm (A, B)
            for (int i = 0; i < products.length; i++) {
                // Đếm số lần sản phẩm A xuất hiện
                productPair.set(products[i]);
                context.write(productPair, one);

                // Đếm các cặp sản phẩm A, B
                for (int j = i + 1; j < products.length; j++) {
                    String pair = products[i] + "," + products[j];
                    productPair.set(pair);
                    context.write(productPair, one);
                }
            }
        }
    }

    // Reducer Class
    public static class ProductReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Map<String, Integer> productCountMap = new HashMap<>();
        private Map<String, Integer> pairCountMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();

            // Kiểm tra nếu key là một sản phẩm đơn lẻ hay là một cặp sản phẩm
            if (keyString.contains(",")) {
                // Cặp sản phẩm (A, B)
                int count = 0;
                for (IntWritable val : values) {
                    count += val.get();
                }
                pairCountMap.put(keyString, count);
            } else {
                // Sản phẩm đơn lẻ (A)
                int count = 0;
                for (IntWritable val : values) {
                    count += val.get();
                }
                productCountMap.put(keyString, count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Tính xác suất có điều kiện P(B|A) = count(A, B) / count(A)
            for (String pair : pairCountMap.keySet()) {
                String[] products = pair.split(",");
                String A = products[0];
                String B = products[1];

                int countAB = pairCountMap.get(pair);
                int countA = productCountMap.get(A);

                double probability = (double) countAB / countA;
                context.write(new Text(A + " -> " + B), new Text("P(B|A) = " + probability));
            }
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ProductProbability <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Conditional Probability");

        job.setJarByClass(ProductProbability.class);
        job.setMapperClass(ProductMapper.class);
        job.setReducerClass(ProductReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}