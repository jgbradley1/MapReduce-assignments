import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    // Mapper: emits (token, 1) for every word occurrence.
    private static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Reuse objects to save overhead of object creation.
        private final static IntWritable ONE = new IntWritable(1);
        private final static Text WORD = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                WORD.set(itr.nextToken());
                context.write(WORD, ONE);
            }
        }
    }
    
    // Reducer: sums up all the counts.
    private static class MyReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Reuse objects.
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }
    
    
    
 // Mapper: emits (token, 1) for every word occurrence.
    private static class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        // Reuse objects to save overhead of object creation.
        private final static IntWritable ONE = new IntWritable(1);
        private final static Text BIGRAM = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = ((Text) value).toString();
            StringTokenizer itr1 = new StringTokenizer(line);
            StringTokenizer itr2 = new StringTokenizer(line);
            // start second iterator on second word in line
            if (itr2.hasMoreTokens())
                itr2.nextToken();
            
            String bigram;
            
            while (itr1.hasMoreTokens()) {
                bigram = itr1.nextToken();
                if (itr2.hasMoreTokens()) {
                    bigram += "_" + itr2.nextToken();
                    
                    BIGRAM.set(bigram);
                    context.write(BIGRAM, ONE);
                }
            }
        }
    }

    // Reducer: sums up all the counts.
    private static class MyReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Reuse objects.
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }
    
    
    
    
    
    /**
     * Creates an instance of this tool.
     */
    public PairsPMI() {}

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
        
        
        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);
        
        //#################################################################################
        // Job 1 Configuration
        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1);
        job1.setJobName(PairsPMI.class.getSimpleName());
        job1.setJarByClass(PairsPMI.class);

        job1.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path("temp"));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(MyMapper1.class);
        job1.setCombinerClass(MyReducer1.class);
        job1.setReducerClass(MyReducer1.class);
        
        
        //#################################################################################
        // Job 2 Configuration
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2);
        job2.setJobName(PairsPMI.class.getSimpleName());
        job2.setJarByClass(PairsPMI.class);

        job2.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(MyMapper2.class);
        job2.setCombinerClass(MyReducer2.class);
        job2.setReducerClass(MyReducer2.class);
        //#################################################################################
        
        
        // Delete the output directories if they exists already.
        Path outputDir = new Path("temp");
        FileSystem.get(conf1).delete(outputDir, true);
        
        outputDir = new Path(outputPath);
        FileSystem.get(conf2).delete(outputDir, true);
        
        

        long startTime = System.currentTimeMillis();
        if (job1.waitForCompletion(true)) {
            LOG.info("Job #1 Finished");
            if (job2.waitForCompletion(true)) {
                LOG.info("Job #2 Finished");
                LOG.info("Job (#1 and #2) Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }
            else {
                LOG.info("ERROR - Job #2 did not finish");
            }
        }
        else {
            LOG.info("ERROR - Job #1 did not finish");
        }

        return 0;
    }
    
    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }
}