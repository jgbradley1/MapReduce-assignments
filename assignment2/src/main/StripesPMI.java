import java.io.IOException;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class StripesPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);

    // Mapper: emits ...
    private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapSIW> {

        // Reuse objects to save overhead of object creation.
        private static final Text KEY = new Text();


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            HMapSIW MAP = new HMapSIW();

            String line = ((Text) value).toString();
            StringTokenizer itr = new StringTokenizer(line);

            // each mapper must have it's own copy of docWords since docWords only maintains information about 1 doc at a time
            List<String> docWords = new LinkedList<String>();

            // Clean out all duplicate words in the document
            while (itr.hasMoreTokens()) {
                String term = itr.nextToken();

                if (!docWords.contains(term)) {
                    docWords.add(term);
                }
            }

            // sort docWords alphabetically -- a requirement so that word pairs are not double counted
            Collections.sort(docWords);

            // Emit word counts and bigram counts
            for (int i = 0; i < docWords.size(); i++) {
                String term = docWords.get(i);

                // This will count P(x)
                MAP.put("*",  1);

                // This will count P(x, y)
                for (int j = i+1; j < docWords.size(); j++) {
                    MAP.put(docWords.get(j), 1);
                }

                //if (term.equals("god")) {
                //    System.out.println("# of words found with god=" + MAP.size());
                //}
                KEY.set(term);
                context.write(KEY, MAP);

                MAP.clear();

            }

            // Clean out entire hashmap
            docWords.clear();
            MAP.clear();
        }
    }

    // Reducer: sums up all the counts for each pair.
    // if pair is of the form (x, *), then will emit a total count of x in the vocabulary
    // if pair is of the form (x, y), then will emit the calculation P(x,y)/P(x)
    private static class MyReducer extends Reducer<Text, HMapSIW, PairOfStrings, FloatWritable> {

        // Reuse objects.
        private final static PairOfStrings KEY = new PairOfStrings();
        private final static FloatWritable VALUE = new FloatWritable(0.0f);
        private float marginal = 0.0f;

        @Override
        public void reduce(Text key, Iterable<HMapSIW> values, Context context)
                throws IOException, InterruptedException {

            // Sum up values.
            HashMap<String, Integer> globalDocCount = new HashMap<String, Integer>();

            float sum = 0;
            for (HMapSIW hmap : values) {

                sum += hmap.get("*");

                for (String word : hmap.keySet()) {
                    if (globalDocCount.containsKey(word)) {
                        globalDocCount.put(word, globalDocCount.get(word)+hmap.get(word));
                    }
                    else 
                        globalDocCount.put(word, 1);
                }
            }
            marginal = sum;

            //if (key.toString().equals("god"))
            //    System.out.println("\n\n god - " + marginal + "\n\n");

            // emit final count of all (x, *) pairs
            KEY.set(key.toString(), "*");
            VALUE.set(marginal);
            context.write(KEY,  VALUE);

            for (String word : globalDocCount.keySet()) {

                // emit P(x,y)/P(x) for each (x, y) pair
                if (globalDocCount.get(word) >= 10 && word.compareTo("*") != 0) {
                    KEY.set(key.toString(), word);

                    float p_x = marginal/156215.0f;
                    float p_xy = globalDocCount.get(word)/156215.0f;
                    //float temp = globalDocCount.get(word);

                    VALUE.set(p_xy/p_x);
                    //VALUE.set(temp);

                    context.write(KEY, VALUE);
                }
            }
        }
    }

    protected static class MyCombiner extends Reducer<Text, HMapSIW, Text, HMapSIW> {

        private static final HMapSIW map = new HMapSIW();
        @Override
        public void reduce(Text key, Iterable<HMapSIW> values, Context context)
                throws IOException, InterruptedException {

            // Sum up values
            // make sure map is cleared out first
            map.clear();
            for (HMapSIW oldMap : values) {
                map.plus(oldMap);
            }
            context.write(key, map);
        }
    }

    protected static class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
        @Override
        public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }





    // Mapper: emits (word, 1) for each unique word in a document (i.e. will not double count words in a doc)
    private static class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, Text> {

        // Reuse objects to save overhead of object creation.
        private static final PairOfStrings KEY = new PairOfStrings();
        private static final Text VALUE = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            PairOfStrings keyy = new PairOfStrings();

            String[] val = value.toString().split("\\t");

            String left = val[0].substring(1, val[0].indexOf(','));
            String right = val[0].substring(val[0].indexOf(" ")+1, val[0].indexOf(")"));

            keyy.set(left,  right);

            if (keyy.getRightElement().equals("*")) {
                // we're dealing with a word count of y
                // emit [(y, *), "value"]
                KEY.set(keyy.getLeftElement(), keyy.getRightElement());
                VALUE.set(val[1]);

                context.write(KEY, VALUE);
            }
            else {
                // we're dealing with an actual bigram pair of the form (x, y)
                // emit [(y, __), "(y, x) value)"]
                KEY.set(keyy.getLeftElement(), "__");
                VALUE.set(keyy.toString()+"-"+val[1]);

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
                            pmi = (float)Math.log(pmi);
                            VALUE.set(pmi);

                            context.write(KEY, VALUE);
                        }
                    }
                }
            }
            catch (NumberFormatException e) {
                System.out.println("\n\nSOME FLOAT NUMBER CONVERSION SCREWED UP -- CHECK IT OUT\n\n");
            }
        }
    }



    /**
     * Creates an instance of this tool.
     */
    public StripesPMI() {}

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


        LOG.info("Tool: " + StripesPMI.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        //#################################################################################
        // Job 1 Configuration
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(StripesPMI.class.getSimpleName());
        job.setJarByClass(StripesPMI.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path("stripes-temp"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HMapSIW.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);


        //#################################################################################
        // Job 2 Configuration
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2);
        job2.setJobName(StripesPMI.class.getSimpleName());
        job2.setJarByClass(StripesPMI.class);

        job2.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job2, new Path("stripes-temp"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(FloatWritable.class);

        job2.setMapperClass(MyMapper2.class);
        job2.setPartitionerClass(MyPartitioner.class);
        job2.setReducerClass(MyReducer2.class);
        //#################################################################################


        // Delete the output directories if they exists already.
        Path outputDir = new Path("stripes-temp");
        FileSystem.get(conf).delete(outputDir, true);

        outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);


        long startTime = System.currentTimeMillis();
        if (job.waitForCompletion(true)) {
            LOG.info("Job #1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            if (job2.waitForCompletion(true)) {
                LOG.info("Job #1 and #2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

                // delete the temporary intermediate data that was generated between jobs
                outputDir = new Path("stripes-temp");
                FileSystem.get(conf).delete(outputDir, true);
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
        ToolRunner.run(new StripesPMI(), args);
    }
}