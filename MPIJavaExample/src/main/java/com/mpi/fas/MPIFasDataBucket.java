package com.mpi.fas;

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
public class MPIFasDataBucket {
    private static String filepath1 = "/home/mpiuser/corpus_1000_article_num.txt";
    private static int tokensize = 21;

    public static void main(String[] args) throws IOException, MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.getRank(), size = MPI.COMM_WORLD.getSize();
        BufferedReader br = new BufferedReader(new FileReader(filepath1));

        List<String> S = new ArrayList<>();

        String str;
        long readtime = System.currentTimeMillis();
        if(rank != 0) {
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
                    FasDataMake modkey = new FasDataMake();
                    modkey.getTokenUsingData(rank, hash1[0], tokensize, S);
                }
            }

        }
        System.out.println(rank + ": Total find pattern time = " + (System.currentTimeMillis() - PatternFindTime) + " ms");

        System.out.println(rank + ": Total running time = "+ (System.currentTimeMillis()-readtime)+" ms");
        MPI.Finalize();
    }
}
