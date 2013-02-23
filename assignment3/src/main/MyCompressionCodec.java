import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;


public class MyCompressionCodec implements CompressionCodec {
    
    @Override
    public Compressor createCompressor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Decompressor createDecompressor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream arg0, Decompressor arg1)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream arg0, Compressor arg1)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getDefaultExtension() {
        // TODO Auto-generated method stub
        return null;
    }

}
