import java.nio.*;
import java.io.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

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
                                                        // memory [cite: 15]
    private static final int RADIX = 8; // Number of bits to process per pass
    private static final int K = 4; // Number of passes for a 32-bit key (32 / 8
                                    // = 4)
    private static final int R = 1 << RADIX; // Number of buckets (2^8 = 256)

    // --- I/O and Statistics Fields ---
    private RandomAccessFile inputFile; // The file to read from (starts as
                                        // original file)
    private RandomAccessFile outputFile; // The file to write to (starts as temp
                                         // file)
    private PrintWriter statsWriter; // For outputting statistics [cite: 53]
    private long fileSize;
    private long numRecords;
    private long numReads; // Number of disk reads
    private long numWrites; // Number of disk writes

    // --- Memory Pool and Buffer Management ---
    private byte[] memoryPool; // The 900,000 byte array
    private ByteBuffer byteBuffer; // Wrapper for the memoryPool for I/O
    private RandomAccessFile tempFile; // The actual temporary file handle
    private String tempFileName = "tempfile.bin"; // Name for the temporary file

// // --- Phase 2 Buffer Configuration ---
// // We will give each of the 256 output buffers a reasonable size
// // (e.g., 219 records, or 1752 bytes).
// private static final int RECORDS_PER_OUTPUT_BUFFER = 219;
// private static final int OUTPUT_BUFFER_BYTES = RECORDS_PER_OUTPUT_BUFFER
// * RECORD_SIZE; // 1752 bytes
//
// // The 256 output buffers will take up this much space
// private static final int OUTPUT_BUFFER_POOL_SIZE = R * OUTPUT_BUFFER_BYTES;
//
// // The *rest* of the memory pool will be used for the single input buffer
// // in Phase 2
// private static final int INPUT_BUFFER_SIZE_PHASE2 = MEMORY_POOL_SIZE
// - OUTPUT_BUFFER_POOL_SIZE; // 900,000 - 448,512 = 451,488 bytes

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
     * Create a new Radix object.
     * * @param theFile
     * The RandomAccessFile to be sorted
     * * @param s
     * The stats PrintWriter
     *
     * @throws IOException
     */
// public Radix(RandomAccessFile theFile, PrintWriter s) throws IOException {
// this.statsWriter = s;
// this.inputFile = theFile;
// this.fileSize = theFile.length();
// this.numReads = 0;
// this.numWrites = 0;
//
// // 1. Initialize Memory Pool
// this.memoryPool = new byte[MEMORY_POOL_SIZE];
//
// // 2. Create and Open Temporary File
// tempFile = new RandomAccessFile(tempFileName, "rw");
// tempFile.setLength(fileSize); // Pre-allocate the space
// this.outputFile = tempFile;
//
// // 3. Write Initial Statistics
// statsWriter.println("Memory Pool Size: " + MEMORY_POOL_SIZE + " bytes");
//
// // 4. Start Sort
// radixSort();
//
// // 5. Clean up temporary file (Crucial!)
// tempFile.close();
// Files.delete(Paths.get(tempFileName));
// }

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

            Arrays.fill(count, 0);
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
            // ---

            inputFile.seek(0);
            bytesRemaining = fileSize;

            // This array will hold a small number of records to be written to
            // the output buffer
            // Note: We MUST write to the destination *position*, so we can't
            // use a simple sequential write.
            // The most efficient and stable method requires a dedicated output
            // block for each bucket,
            // which exceeds the size of 'count' and is a huge $R$-way
            // merge/buffer-pool design.

            // **Compromise:** Use a small, fixed-size cache of output blocks to
            // reduce excessive seeking.
            // A single record write per bucket is necessary if we stick to the
            // provided structure.

            // *Reverting to the previous logic but ensuring full block reads
            // for correctness*

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
                    numWrites++; // Unoptimized write-per-record - REMAINS A
                    // CRITICAL PERFORMANCE ISSUE [cite: 60]

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
     * Do a Radix sort
     *
     * @throws IOException
     */
// private void radixSort() throws IOException {
// int[] count = new int[R];
// long[] destBytePosition = new long[R];
//
// // A temporary 8-byte array to move one record at a time (memory to
// // memory)
// byte[] record = new byte[RECORD_SIZE];
//
// // --- Phase 2 Buffer Setup (Allocated once, re-used in loop) ---
// // Input buffer for Phase 2 (uses the *first* part of the pool)
// ByteBuffer inputBufferPhase2 = ByteBuffer.wrap(memoryPool, 0,
// INPUT_BUFFER_SIZE_PHASE2);
//
// // Array of 256 output buffers (uses the *second* part of the pool)
// ByteBuffer[] outputBuffers = new ByteBuffer[R];
// for (int i = 0; i < R; i++) {
// outputBuffers[i] = ByteBuffer.wrap(memoryPool,
// INPUT_BUFFER_SIZE_PHASE2 + (i * OUTPUT_BUFFER_BYTES),
// OUTPUT_BUFFER_BYTES);
// }
//
// // Loop for K passes (4 passes for 32-bit key with 8-bit radix)
// for (int pass = 0; pass < K; pass++) {
//
// // --- Phase 1: Count Records (Use *ENTIRE* 900k pool for input) ---
// ByteBuffer inputBufferPhase1 = ByteBuffer.wrap(memoryPool, 0,
// MEMORY_POOL_SIZE);
//
// Arrays.fill(count, 0);
// inputFile.seek(0);
// long bytesRemaining = fileSize;
//
// while (bytesRemaining > 0) {
// int readSize = (int)Math.min(bytesRemaining, MEMORY_POOL_SIZE); // Use
// // full
// // pool
// inputBufferPhase1.clear();
// inputBufferPhase1.limit(readSize);
//
// inputFile.readFully(memoryPool, 0, readSize);
// numReads++; // Count a disk read
// IntBuffer intBuffer = inputBufferPhase1.asIntBuffer();
//
// int rtok = pass * RADIX;
// int mask = R - 1;
//
// int numRecordsInBlock = readSize / RECORD_SIZE;
// for (int i = 0; i < numRecordsInBlock; i++) {
// int key = intBuffer.get(2 * i);
// int digit = (key >> rtok) & mask;
// count[digit]++;
// }
// bytesRemaining -= readSize;
// }
//
// // 1.2. Prefix Sum: Calculate starting byte position in the output
// // file
// long currentTotal = 0;
// for (int j = 0; j < R; j++) {
// destBytePosition[j] = currentTotal;
// currentTotal += (long)count[j] * RECORD_SIZE;
// }
//
// // --- Phase 2: Distribution Pass (Use *ASYMMETRIC* buffer split)
// // ---
//
// inputFile.seek(0);
// bytesRemaining = fileSize;
//
// while (bytesRemaining > 0) {
// int readSize = (int)Math.min(bytesRemaining,
// INPUT_BUFFER_SIZE_PHASE2); // Use Phase 2 input size
// inputBufferPhase2.clear();
// inputBufferPhase2.limit(readSize);
//
// inputFile.readFully(memoryPool, 0, readSize);
// numReads++; // Read of input block
//
// IntBuffer intBuffer = inputBufferPhase2.asIntBuffer();
//
// int rtok = pass * RADIX;
// int mask = R - 1;
// int numRecordsInBlock = readSize / RECORD_SIZE;
//
// // Process each record in the current buffer
// for (int i = 0; i < numRecordsInBlock; i++) {
// int key = intBuffer.get(2 * i);
// int digit = (key >> rtok) & mask;
//
// ByteBuffer destBuffer = outputBuffers[digit];
//
// // Check if the destination buffer is full
// if (!destBuffer.hasRemaining()) { // More robust check
// flushBuffer(destBuffer, digit, destBytePosition);
// destBytePosition[digit] += OUTPUT_BUFFER_BYTES;
// }
//
// // Get the 8-byte record from the input buffer
// inputBufferPhase2.position(i * RECORD_SIZE);
// inputBufferPhase2.get(record);
//
// // Add the record to its destination buffer
// destBuffer.put(record);
// }
// bytesRemaining -= readSize;
// }
//
// // --- Phase 2.5: Final Flush ---
// // Flush all remaining data from output buffers to the file
// for (int i = 0; i < R; i++) {
// if (outputBuffers[i].position() > 0) {
// flushBuffer(outputBuffers[i], i, destBytePosition);
// }
// }
// outputFile.getChannel().force(true); // Force writes to disk
//
// // --- Phase 3: Swap Input and Output Files ---
// swapFiles();
// }
//
// // Write final statistics
// statsWriter.println("Num Disk Reads: " + numReads);
// statsWriter.println("Num Disk Writes: " + numWrites);
// }


    /**
     * Flushes a single output buffer to its correct position in the output
     * file.
     * * @param buffer
     * The ByteBuffer to flush
     * 
     * @param digit
     *            The bucket index (0-255) of this buffer
     * @param destBytePosition
     *            The array of file pointers
     * @throws IOException
     */
// private void flushBuffer(
// ByteBuffer buffer,
// int digit,
// long[] destBytePosition)
// throws IOException {
//
// outputFile.seek(destBytePosition[digit]);
// buffer.flip(); // Prepare buffer for reading (writing to channel)
//
// // Use channel write, as it's generally more efficient with ByteBuffers
// outputFile.getChannel().write(buffer);
// numWrites++; // Count one disk write
//
// buffer.clear(); // Prepare buffer for writing (putting records)
// }


    private void swapFiles() {
        RandomAccessFile temp = inputFile;
        inputFile = outputFile;
        outputFile = temp;
    }
}
