import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ExtractHourlyCountsAll extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(ExtractHourlyCountsAll.class);

    // Mapper: emits (token, 1) for every word occurrence.
    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Reuse objects to save overhead of object creation.
        private final static Text KEY = new Text();
        private final static IntWritable VALUE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();
            String lineContents[] = line.split("\t");
            if (lineContents.length > 1) {

                String timestamp[] = lineContents[1].split(" ");

                /*
                 * timestamp[0] - day of the week (Sun, Mon, Tue, Wed, Thu, Fri, Sat)
                 * timestamp[1] - month of the year (Jan, Feb, Mar, Apr, May, Jun, etc.)
                 * timestamp[2] - day of the month (numerical)
                 * timestamp[3] - time in format hh:mm:ss
                 * timestamp[4...6] - information not necessary for calculations
                 */
                
                String tmp = "";

                if (timestamp[1].equals("Jan") || timestamp[1].equals("Feb")) {
                    if (timestamp[1].equals("Jan")) {
                        tmp = "1/";
                    }
                    else {
                        tmp = "2/";
                    }
                    
                    tmp += timestamp[2];
                    String time[] = timestamp[3].split(":");
                    tmp += "\t" + time[0];

                    System.out.println(tmp);
                    KEY.set(tmp);

                    context.write(KEY,  VALUE);
                }
            }
            else {
                System.out.println("ERROR: line parse problem");
            }
        }
    }

    // Reducer: sums up all the counts.
    private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Reuse objects.
        private final static IntWritable VALUE = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            VALUE.set(sum);
            context.write(key, VALUE);
        }
    }


    private static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final static IntWritable VALUE = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            VALUE.set(sum);
            context.write(key, VALUE);
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public ExtractHourlyCountsAll() {}

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        // Set default values
        String inputPath = "/user/shared/tweets2011/tweets2011.txt";
        String outputPath = "jgbradley1-all";
        int reduceTasks = 1;

        LOG.info("Tool: " + ExtractHourlyCountsAll.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(ExtractHourlyCountsAll.class.getSimpleName());
        job.setJarByClass(ExtractHourlyCountsAll.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ExtractHourlyCountsAll(), args);
    }
}