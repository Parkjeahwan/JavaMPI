package com.mpi.fair;

import mpi.MPI;
import mpi.MPIException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by parkjh on 16. 8. 29.
 */
public class MPIFairData {
    private static String filepath1 = "/home/mpiuser/corpus_1000_article.txt";
    private static String filepath2 = "/home/mpiuser/MPI_ncom/newfasdataoutput.txt";
    //Number of bucket
    private static int tokensize = 679;

    public static void main(String[] args) throws IOException, MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.getRank(), size = MPI.COMM_WORLD.getSize();

        List<String> S = new ArrayList<String>();

        String str;
        if (rank != 0) {
            long readtime = System.currentTimeMillis();
            BufferedReader br = new BufferedReader(new FileReader(filepath1));
            while ((str = br.readLine()) != null) {
                S.add(str);
            }
            br.close();
            System.out.println(rank + ": Reading Time = " + (System.currentTimeMillis() - readtime) + " ms");
        }

        long PatternFindTime = System.currentTimeMillis();
        int[] hash1 = new int[1];
        if (rank == 0) {
            int sendId = hash1[0];
            for (int i = 0; i < tokensize; i ++) {
                MPI.COMM_WORLD.recv(hash1, 1, MPI.INT, MPI.ANY_SOURCE, 1);
                sendId = hash1[0];
                hash1[0] = i;
                MPI.COMM_WORLD.send(hash1, 1, MPI.INT, sendId, 1);
            }

            for (int i = 0; i < size-1; i++) {
                MPI.COMM_WORLD.recv(hash1, 1, MPI.INT, MPI.ANY_SOURCE, 1);
                sendId = hash1[0];
                hash1[0] = -1;
                MPI.COMM_WORLD.send(hash1, 1, MPI.INT, sendId, 1);
            }
        } else {
            boolean checkLoop = true;
            while (checkLoop) {
                hash1[0] = rank;
                MPI.COMM_WORLD.send(hash1, 1, MPI.INT, 0, 1);
                MPI.COMM_WORLD.recv(hash1, 1, MPI.INT, 0, 1);
                if ( hash1[0] < 0) {
                    checkLoop = false;
                } else {
                    ABPairSy modkey = new ABPairSy();
                    modkey.Findpair(rank, hash1[0], tokensize, S);
                }
            }

        }
        System.out.println(rank + ": Find pattern time = " + (System.currentTimeMillis() - PatternFindTime) + " ms");
        MPI.Finalize();
    }
}
