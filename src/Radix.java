import java.nio.*;
import java.io.*;

import java.nio.file.Files;
import java.nio.file.Paths;
// import java.util.Arrays;

// The Radix Sort implementation
// -------------------------------------------------------------------------
/**
 *
 * @author Ben Blucher, Austin Kane (benblucher, austink23)
 * @version 10.21.2025
 */
public class Radix {

    private static final int KEY_SIZE = 4; // 4-byte integer key
    private static final int RECORD_SIZE = 8; // 8-byte record (key + data)
    private static final int MEMORY_POOL_SIZE = 900000; // 900,000 bytes working
                                                        // memory
    private static final int RADIX = 8; // Number of bits to process per pass
    private static final int K = 4; // Number of passes for a 32-bit key (32 / 8
                                    // = 4)
    private static final int R = 1 << RADIX; // Number of buckets (2^8 = 256)

    // --- I/O and Statistics Fields ---
    private RandomAccessFile inputFile; // The file to read from (starts as
                                        // original file)
    private RandomAccessFile outputFile; // The file to write to (starts as temp
                                         // file)
    private PrintWriter statsWriter; // For outputting statistics
    private long fileSize;
    private long numRecords;
    private long numReads; // Number of disk reads
    private long numWrites; // Number of disk writes

    // --- Memory Pool and Buffer Management ---
    private byte[] memoryPool; // The 900,000 byte array
    private ByteBuffer byteBuffer; // Wrapper for the memoryPool for I/O
    private RandomAccessFile tempFile; // The actual temporary file handle
    private String tempFileName = "tempfile.bin"; // Name for the temporary file

    private static final int HALF_POOL_SIZE = MEMORY_POOL_SIZE / 2;

    /**
     * Create a new Radix object.
     * * @param theFile
     * The RandomAccessFile to be sorted
     * 
     * @param s
     *            The stats PrintWriter
     *
     * @throws IOException
     */
    public Radix(RandomAccessFile theFile, PrintWriter s) throws IOException {
        // Basic initializations
        this.statsWriter = s;
        this.inputFile = theFile;
        this.fileSize = theFile.length();
        this.numRecords = this.fileSize / RECORD_SIZE;
        this.numReads = 0;
        this.numWrites = 0;

        // 1. Initialize Memory Pool
        this.memoryPool = new byte[MEMORY_POOL_SIZE];
        this.byteBuffer = ByteBuffer.wrap(memoryPool);

        // 2. Create and Open Temporary File
        // The temp file's initial size should be 0, but it will grow to the
        // size of the input file.
        tempFile = new RandomAccessFile(tempFileName, "rw");
        tempFile.setLength(fileSize); // Pre-allocate the space to match the
        // input file size
        this.outputFile = tempFile;

        // 3. Write Initial Statistics
        statsWriter.println("Memory Pool Size: " + MEMORY_POOL_SIZE + " bytes ("
            + (MEMORY_POOL_SIZE / RECORD_SIZE) + " records)");

        // 4. Start Sort
        radixSort();

        // 5. Clean up temporary file (Crucial!)
        tempFile.close();
        Files.delete(Paths.get(tempFileName));
    }


    /**
     * Do a Radix sort
     *
     * @throws IOException
     */
    private void radixSort() throws IOException {
        int[] count = new int[R]; // Count[i] stores number of records for bin i

        // Buffers for I/O
        ByteBuffer inputBlock = ByteBuffer.wrap(memoryPool, 0, HALF_POOL_SIZE);
        ByteBuffer outputBlock = ByteBuffer.wrap(memoryPool, HALF_POOL_SIZE,
            HALF_POOL_SIZE);

        // Loop for K passes (4 passes for 32-bit key with 8-bit radix)
        for (int pass = 0; pass < K; pass++) {
            // --- Phase 1: Count Records and Calculate Prefix Sum ---

            for (int i = 0; i < R; i++) {
                count[i] = 0;
            }
            inputFile.seek(0);
            long bytesRemaining = fileSize;

            // 1.1. Count the number of records for each bin (using the first
            // HALF_POOL_SIZE for read buffer)
            while (bytesRemaining > 0) {
                int readSize = (int)Math.min(bytesRemaining, HALF_POOL_SIZE);

                inputBlock.clear();

                // Read block from disk
                // We use readFully for reliable block reads (it handles EOF
                // checks if file size is correct)
                inputFile.readFully(memoryPool, 0, readSize);
                numReads++; // Count a disk read

                inputBlock.limit(readSize);
                IntBuffer intBuffer = inputBlock.asIntBuffer();

                int rtok = pass * RADIX;
                int mask = R - 1;

                // continue reading
                for (int i = 0; i < readSize / RECORD_SIZE; i++) {
                    int key = intBuffer.get(2 * i);
                    int digit = (key >> rtok) & mask;
                    count[digit]++;
                }
                bytesRemaining -= readSize;
            }

            // 1.2. Prefix Sum: Calculate starting byte position in the output
            // file
            long currentTotal = 0;
            long[] destBytePosition = new long[R];
            for (int j = 0; j < R; j++) {
                destBytePosition[j] = currentTotal; // Store the starting byte
                // position
                currentTotal += (long)count[j] * RECORD_SIZE;
            }

            // --- Phase 2: Distribution Pass (Read from Input, Write to Output)

            inputFile.seek(0);
            bytesRemaining = fileSize;

            while (bytesRemaining > 0) {
                int readSize = (int)Math.min(bytesRemaining, HALF_POOL_SIZE);

                inputBlock.clear();
                inputFile.readFully(memoryPool, 0, readSize);
                numReads++; // Read of input block

                inputBlock.limit(readSize);
                IntBuffer intBuffer = inputBlock.asIntBuffer();

                int rtok = pass * RADIX;
                int mask = R - 1;

                // Process each record in the current buffer
                for (int i = 0; i < readSize / RECORD_SIZE; i++) {
                    int key = intBuffer.get(2 * i);
                    int digit = (key >> rtok) & mask;

                    // Get the 8-byte record from the input buffer
                    byte[] record = new byte[RECORD_SIZE];
                    inputBlock.position(i * RECORD_SIZE);
                    inputBlock.get(record);

                    // Write the record to its destination in the output file
                    outputFile.seek(destBytePosition[digit]);
                    outputFile.write(record);
                    numWrites++;

                    // Update the next write position for this bucket
                    destBytePosition[digit] += RECORD_SIZE;
                }
                bytesRemaining -= readSize;
            }
            outputFile.getChannel().force(true); // Force writes to disk

            // --- Phase 3: Swap Input and Output Files ---
            // Swap file roles to avoid copying B back to A
            swapFiles();
        }

        swapFiles();
        // Now the original file (the one that `RadixProj` keeps open) is
        // `inputFile`, and it holds the sorted data.

        // Write final statistics
        statsWriter.println("Num Disk Reads: " + numReads);
        statsWriter.println("Num Disk Writes: " + numWrites);
    }


    /**
     * Helper method for swapping files between input (permanent) and
     * temporary.
     */
    private void swapFiles() {
        RandomAccessFile temp = inputFile;
        inputFile = outputFile;
        outputFile = temp;
    }
}
