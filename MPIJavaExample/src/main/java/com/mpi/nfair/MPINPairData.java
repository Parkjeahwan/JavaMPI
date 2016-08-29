package com.mpi.nfair;

import mpi.MPI;
import mpi.MPIException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by parkjh on 16. 8. 29.
 */
public class MPINPairData {
    private static String filepath1 = "/home/mpiuser/corpus_mecab_pr.txt";
    private static String filepath2 = "/home/mpiuser/MPI_npair/H10Pairdata.txt";

    public static void main(String[] args) throws IOException, MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.getRank(), size = MPI.COMM_WORLD.getSize();

        BufferedReader br = new BufferedReader(new FileReader(filepath1));

        HashMap<String, Integer> newFasdata = new HashMap<String, Integer>();
        List<String> S = new ArrayList<String>();

        long times = System.currentTimeMillis();
        String str;
        while ((str = br.readLine()) !=null) {
            S.add(str);
        }
        br.close();
        System.out.println(rank + ": file reading time = " + (System.currentTimeMillis()-times) + "ms");

        long hashtimes = System.currentTimeMillis();

        for (int k = 0; k < S.size(); k++) {
            if (k % 100000 == 0 && k != 0)  System.out.print(k + " ");
            if (k == 1000000) {
                System.out.println("");
                break;
            }

            String m[] = S.get(k).split(" ");
            int u = 10;

            for (int k0=1;k0<=u;k0++) {
                HashSet<String> checkm1 = new HashSet <String> ();
                for (int i=0;i<m.length-k0+1;i++) {
                    char ch[] = m[i].toCharArray();
                    if ((((int)ch[0] + (int)ch[2])>>3)%size != rank)
                        continue;

                    HashSet <String> checkm2 = new HashSet<String>();
                    String m1 = "";
                    for (int ii=i;ii<i+k0;ii++) m1 += m1.equals("")? m[ii] : " "+m[ii];

                    if (checkm1.contains(m1)) continue;
                    else checkm1.add(m1);

                    for (int k1 = 1; k1 <= u; k1++) {
                        String m2 = "";
                        for (int j = i + k0; i < m.length - k0 - k1 + 1 && j < i + k0 + k1; j++) m2 += m2.equals("") ? m[j] : " " + m[j];
                        if(m2.equals("")) continue;

                        if (m1.equals(m2) || checkm2.contains(m2)) continue;
                        else checkm2.add(m2);

                        String keys = m1 + " ##SP " + m2;
                        if (newFasdata.containsKey(keys)){
                            newFasdata.put(keys, newFasdata.get(keys) + 1);
                        } else {
                            newFasdata.put(keys, 1);
                        }
                    }
                }
            }
        }
        System.out.println(rank + ": hashmap make time = " + (System.currentTimeMillis()-hashtimes) + "ms");

        long times1 = System.currentTimeMillis();
        BufferedWriter bw = new BufferedWriter(new FileWriter(filepath2 + "." + rank, false));

        for(String key: newFasdata.keySet()) {
            bw.write(key + "\t" + newFasdata.get(key) + "\n");
        }
        bw.close();
        System.out.println(rank + ": Data write time = " + (System.currentTimeMillis()-times1) + " ms");

        System.out.println(rank + ": Total running time = " + (System.currentTimeMillis()-times) + " ms");
        MPI.Finalize();
    }
}
