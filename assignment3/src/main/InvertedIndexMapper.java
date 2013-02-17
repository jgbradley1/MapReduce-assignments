import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfInts;

// Mapper: emits (token, 1) for every word occurrence.
class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
    private static final Logger LOG = Logger.getLogger(InvertedIndexMapper.class);

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