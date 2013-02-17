import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfInts;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    // Mapper: emits (token, 1) for every word occurrence.
    private static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {

        // Reuse objects to save overhead of object creation.
        private final static PairOfInts PAIR = new PairOfInts();
        private final static Text WORD = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();
            String[] terms = line.split("\\s+");
            String word;
            int tf; // term frequency

            for (int i = 0; i < terms.length; i++) {
                word = terms[i];
                tf = 0;
                for (int j = 0; j < terms.length; j++) {
                    if (word.compareTo(terms[j]) == 0) {
                        tf++;
                    }
                }

                WORD.set(word);
                PAIR.set(Integer.parseInt(key.toString()), tf);
                context.write(WORD, PAIR);
            }
        }
    }
    
    // Reducer: sums up all the counts.
    private static class MyReducer extends Reducer<Text, PairOfInts, Text, Text> {

        // Reuse objects.
        private final static Text PostingsList = new Text();

        @Override
        public void reduce(Text key, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {

            if (!key.toString().isEmpty()) {
                // Sum up values.
                int df = 0; // document frequency
                String postingList = "";
                boolean firstPair = true;
                for (PairOfInts docWordCount : values) {
                    df += docWordCount.getRightElement();
                    if (!firstPair)
                        postingList += ", " + docWordCount.toString();
                    else {
                        postingList += docWordCount.toString();
                        firstPair = false;
                    }
                }
                PostingsList.set(": " + df + " : " + postingList);
                context.write(key, PostingsList);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public BuildInvertedIndexCompressed() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(MyReducer.class);
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
        ToolRunner.run(new BuildInvertedIndexCompressed(), args);
    }
}