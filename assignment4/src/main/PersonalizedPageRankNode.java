import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for Personalized PageRank. 
 *
 * @author Joshua Bradley
 */
public class PersonalizedPageRankNode implements Writable {
    public static enum Type {
        Complete((byte) 0),  // PageRank mass and adjacency list.
        Mass((byte) 1),      // PageRank mass only.
        Structure((byte) 2); // Adjacency list only.

        public byte val;

        private Type(byte v) {
            this.val = v;
        }
    };
    
    private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };
    
    private Type type;
    private int nodeid;
    private float pagerank;
    private ArrayListOfIntsWritable adjacenyList;
    private ArrayListOfIntsWritable personalizedPageRankValues;

    public PersonalizedPageRankNode() {}

    public float getPageRank() {
        return pagerank;
    }

    public void setPageRank(float p) {
        this.pagerank = p;
    }
    
    public ArrayListOfIntsWritable getPersonalizedPageRankValues() {
        return personalizedPageRankValues;
    }
    
    public void setPersonalizedPageRankValues(ArrayListOfIntsWritable list) {
        this.personalizedPageRankValues = list;
    }

    public int getNodeId() {
        return nodeid;
    }

    public void setNodeId(int n) {
        this.nodeid = n;
    }

    public ArrayListOfIntsWritable getAdjacenyList() {
        return adjacenyList;
    }

    public void setAdjacencyList(ArrayListOfIntsWritable list) {
        this.adjacenyList = list;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int b = in.readByte();
        type = mapping[b];
        nodeid = in.readInt();

        if (type.equals(Type.Mass)) {
            pagerank = in.readFloat();
            return;
        }

        if (type.equals(Type.Complete)) {
            pagerank = in.readFloat();
        }

        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(in);
        
        personalizedPageRankValues = new ArrayListOfIntsWritable();
        personalizedPageRankValues.readFields(in);
    }

    /**
     * Serializes this object.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type.val);
        out.writeInt(nodeid);

        if (type.equals(Type.Mass)) {
            out.writeFloat(pagerank);
            return;
        }

        if (type.equals(Type.Complete)) {
            out.writeFloat(pagerank);
        }

        adjacenyList.write(out);
        personalizedPageRankValues.write(out);
    }

    @Override
    public String toString() {
        return String.format("{%d %.4f %s %s}",
                nodeid,
                pagerank,
                (adjacenyList == null ? "[]" : adjacenyList.toString(10)),
                (personalizedPageRankValues == null ? "[]" : personalizedPageRankValues.toString(10)));
    }


    /**
     * Returns the serialized representation of this object as a byte array.
     *
     * @return byte array representing the serialized representation of this object
     * @throws IOException
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        write(dataOut);

        return bytesOut.toByteArray();
    }

    /**
     * Creates object from a <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static PersonalizedPageRankNode create(DataInput in) throws IOException {
        PersonalizedPageRankNode m = new PersonalizedPageRankNode();
        m.readFields(in);

        return m;
    }

    /**
     * Creates object from a byte array.
     *
     * @param bytes raw serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static PersonalizedPageRankNode create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
}
