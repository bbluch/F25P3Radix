/**
 * Radix Sort Project main method and support methods:
 * compareFile to test if two files are equal.
 * generateFile to generate a file of a given size and data distribution.
 */

import java.io.*;

/**
 * The class containing the main method.
 *
 * @author Ben Blucher, Austin Kane (benblucher, austink23)
 * @version 10.21.2025
 */

//On my honor:
//
//- I have not used source code obtained from another current or
//former student, or any other unauthorized source, either
//modified or unmodified.
//
//- All source code and documentation used in my program is either my
//original work, or was derived by me from the source code
//published in the textbook for this course. I understand that I am
//permitted to use an LLM tool to assist me with writing project
//code, under the condition that I submit with the project a text
//file that contains the full transcript of my interactions with
//the LLM (showing my prompts and the LLM's response). I understand
//that I am responsible for being able to complete this work
//without the use of LLM assistance.
//
//- I have not discussed coding details about this project with
//anyone other than my partner (in the case of a joint
//submission), instructor, ACM/UPE tutors or the TAs assigned
//to this course. I understand that I may discuss the concepts
//of this program with other students, and that another student
//may help me debug my program so long as neither of us writes
//anything during the discussion or modifies any computer file
//during the discussion. I have violated neither the spirit nor
//letter of this restriction.


public class RadixProj
{

    private static PrintWriter stats;

    /**
     * Main: Process input parameters
     *
     * @param args
     *            The command line parameters
     * @throws IOException
     */
    public static void main(String[] args)
        throws IOException
    {
        if (args.length != 2)
        {
            System.out.println(
                "Usage: RadixProj <data-file-name> <stats-file-name>");
            return;
        }
        File temp = new File(args[0]);
        if (!temp.exists())
        {
            System.out.println("There is no such input file as |" + args[0]
                + "|");
            return;
        }

        stats = new PrintWriter(new BufferedWriter(
                                new FileWriter(args[1], true)));

        try (RandomAccessFile myFile = new RandomAccessFile(args[0], "rw")) {
            stats.println("Sorting " + args[0] + " of size " +
                myFile.length());
            long time1 = System.currentTimeMillis();
            new Radix(myFile, stats);
            long time2 = System.currentTimeMillis();
            myFile.close();

            stats.println("Time is " + (time2 - time1));
            stats.flush();
        }
    }
}
