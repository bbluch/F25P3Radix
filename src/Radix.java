import java.nio.*;
import java.io.*;

// The Radix Sort implementation
// -------------------------------------------------------------------------
/**
 *
 * @author Ben Blucher, Austin Kane (benblucher, austink23)
 * @version 10.21.2025
 */
public class Radix {

    /**
     * Create a new Radix object.
     * 
     * @param theFile
     *            The RandomAccessFile to be sorted
     * @param s
     *            The stats PrintWriter
     *
     * @throws IOException
     */
    public Radix(RandomAccessFile theFile, PrintWriter s) throws IOException {
        Integer[] B = new Integer[A.length];
        int[] count = new int[r]; // Count[i] stores number of records with
                                  // digit value i
        int i, j, rtok;

        for (i = 0, rtok = 1; i < k; i++, rtok *= r) { // For k digits
            for (j = 0; j < r; j++) {
                count[j] = 0;
            } // Initialize count

            // Count the number of records for each bin on this pass
            for (j = 0; j < A.length; j++) {
                count[(A[j] / rtok) % r]++;
            }

            // After processing, count[j] will be index in B for first slot of
            // bin j.
            int total = A.length;
            for (j = r - 1; j >= 0; j--) {
                total -= count[j];
                count[j] = total;
            }

            // Put records into bins, working from left to right
            for (j = 0; j < A.length; j++) {
                B[count[(A[j] / rtok) % r]] = A[j];
                count[(A[j] / rtok) % r] = count[(A[j] / rtok) % r] + 1;
            }
            for (j = 0; j < A.length; j++) {
                A[j] = B[j];
            } // Copy B back
        }
    }


    /**
     * Do a Radix sort
     *
     * @throws IOException
     */
    private void radixSort() throws IOException {
    }
}
