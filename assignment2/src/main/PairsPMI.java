import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    // Mapper: emits (word, 1) for each unique word in a document (i.e. will not double count words in a doc)
    private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {

        // Reuse objects to save overhead of object creation.
        private final static FloatWritable ONE = new FloatWritable(1.0f);
        private static final PairOfStrings PAIR = new PairOfStrings();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // each mapper must have it's own copy of docWords since docWords only maintains information about 1 doc at a time
            HashMap<String, Integer> docWords = new HashMap<String, Integer>();
            List<String> pair = new ArrayList<String>(2);

            String text = value.toString();
            String[] terms = text.split("\\s+");

            // Clean out all duplicate words in the document
            for (int i = 0; i < terms.length; i++) {
                String term = terms[i];

                // skip empty tokens
                if (term.length() == 0)
                    continue;

                if (!docWords.containsKey(term)) {
                    docWords.put(term, 1);
                }
                else {
                    terms[i] = "";
                }
            }

            // Emit word counts and bigram counts
            for (int i = 0; i < terms.length; i++) {
                String term = terms[i];

                // skip empty tokens
                if (term.length() == 0)
                    continue;

                // This will count P(X)
                PAIR.set(term, "*");
                context.write(PAIR, ONE);

                for (int j = i+1; j < terms.length; j++) {

                    // skip empty tokens
                    if (terms[j].length() == 0)
                        continue;

                    pair.add(term);
                    pair.add(terms[j]);

                    Collections.sort(pair);

                    // This will count P(X, Y)
                    PAIR.set(pair.get(0), pair.get(1));
                    context.write(PAIR, ONE);
                    pair.clear();
                }

            }

            // Clean out entire hashmap
            docWords.clear();

        }
    }

    // Reducer: sums up all the counts for each pair.
    // if pair is of the form (x, *), then will emit a total count of x in the vocabulary
    // if pair is of the form (x, y), then will emit the calculation P(x,y)/P(x)
    private static class MyReducer extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

        // Reuse objects.
        private final static FloatWritable PROB = new FloatWritable(0.0f);
        private float marginal = 0.0f;

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            // Sum up values.
            Iterator<FloatWritable> iter = values.iterator();
            int sum = 0;
            float ratio;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            // emit final count of all (x, *) pairs
            if (key.getRightElement().equals("*")) {
                marginal = sum;
                PROB.set(sum);
                context.write(key, PROB);
            }
            else {
                // emit P(x,y)/P(x) for each (x, y) pair

                if (sum >= 10) {
                    float p_x = marginal/156215.0f;
                    float p_xy = sum/156215.0f;

                    PROB.set(p_xy/p_x);
                    context.write(key, PROB);
                }
            }
        }
    }

    protected static class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
        @Override
        public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    protected static class MyCombiner extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<FloatWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }















    // Mapper: emits (word, 1) for each unique word in a document (i.e. will not double count words in a doc)
    private static class MyMapper2 extends Mapper<PairOfStrings, FloatWritable, PairOfStrings, Text> {

        // Reuse objects to save overhead of object creation.
        private static final PairOfStrings KEY = new PairOfStrings();
        private static final Text VALUE = new Text();

        @Override
        public void map(PairOfStrings key, FloatWritable value, Context context)
                throws IOException, InterruptedException {

            if (key.getRightElement().equals("*")) {
                // we're dealing with a word count of y
                // emit [(y, *), "value"]
                KEY.set(key.getLeftElement(), key.getRightElement());
                VALUE.set(value.toString());

                context.write(KEY, VALUE);
            }
            else {
                // we're dealing with an actual bigram pair of the form (x, y)
                // emit [(y, __), "(y, x) value)"]
                KEY.set(key.getLeftElement(), "__");
                VALUE.set(key.toString()+"-"+value.toString());

                context.write(KEY, VALUE);
            }
        }
    }

    // Reducer: sums up all the counts for each word. Will tell how many docs a word has been found in
    private static class MyReducer2 extends Reducer<PairOfStrings, Text, PairOfStrings, FloatWritable> {

        // Reuse objects.
        private static final PairOfStrings KEY = new PairOfStrings();
        private static final FloatWritable VALUE = new FloatWritable();
        private static float p_y = 0.0f;

        @Override
        public void reduce(PairOfStrings key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            Iterator<Text> iter = values.iterator();
            try {
                while (iter.hasNext()) {
                    // multiply each (x,y) pair by 1/p_y
                    if (key.getRightElement().equals("*")) {
                        p_y = Float.parseFloat(iter.next().toString())/156215.0f;
                    }
                    else {  
                        String[] bigram_value = iter.next().toString().split("-");
                        if (bigram_value.length == 2) {
                            String bigramPair = bigram_value[0];

                            String left = bigramPair.substring(1, bigramPair.indexOf(" ")-1);
                            String right = bigramPair.substring(bigramPair.indexOf(" ") + 1, bigramPair.length()-1);
                            KEY.set(left, right);

                            float pmi = Float.parseFloat(bigram_value[1]);
                            pmi *= 1/p_y;
                            VALUE.set(pmi);


                            context.write(KEY, VALUE);
                        }
                    }
                }
            }
            catch (NumberFormatException e) {}
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
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path("temp"));

        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(MyCombiner.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setReducerClass(MyReducer.class);
        

        //#################################################################################
        // Job 2 Configuration
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2);
        job2.setJobName(PairsPMI.class.getSimpleName());
        job2.setJarByClass(PairsPMI.class);

        job2.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(FloatWritable.class);

        job2.setMapperClass(MyMapper2.class);
        //job2.setCombinerClass(MyReducer2.class);
        job2.setPartitionerClass(MyPartitioner.class);
        job2.setReducerClass(MyReducer2.class);
        //#################################################################################
        
        // Delete the output directories if they exists already.
        Path outputDir = new Path("temp");
        FileSystem.get(conf).delete(outputDir, true);

        outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);



        long startTime = System.currentTimeMillis();
        if (job.waitForCompletion(true)) {
            LOG.info("Job #1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            
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