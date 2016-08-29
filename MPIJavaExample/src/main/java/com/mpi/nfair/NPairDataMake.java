package com.mpi.nfair;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by parkjh on 16. 8. 29.
 */
public class NPairDataMake {
    private String filepath2 = "/home/mpiuser/Output_pair/jamoNewdataoutput.txt";

    public void FindNPair (int rank, int tokenNum, int tokenSize, List<String> S) throws IOException {
        HashMap<String, Integer> newFasdata = new HashMap<String, Integer>();

        long hashtime = System.currentTimeMillis();

        for (int h = 0; h < S.size(); h++) {
            char m[] = S.get(h).toCharArray();

            int u = 10;
            for (int k0=1;k0<=u;k0++) {
                //if (h > 1500000) break;

                HashSet<String> checkm1 = new HashSet <String> ();
                for (int i=0;i<m.length-k0+1;i++) {
                    if (k0 == 1) {
                        if (((int)m[i])%tokenSize != tokenNum)
                            continue;
                    } else if (k0 == 2) {
                        int check = (int)m[i] * (int)m[i+1];
                        //if(check < 0) check *= -1;
                        if (check%tokenSize != tokenNum)
                            continue;
                    } else if (k0 == 3) {
                        int check = (int)m[i] + (int)m[i+1] * (int)m[i+2];
                        //if(check < 0) check *= -1;
                        if (check%tokenSize != tokenNum)
                            continue;
                    } else if (k0 == 4) {
                        int check = (int)m[i] * (int)m[i+1] + (int)m[i+2] * (int)m[i+3];
                        //if(check < 0) check *= -1;
                        if (check%tokenSize != tokenNum)
                            continue;
                    } else if (k0 == 5) {
                        int check = (int)m[i] + (int)m[i+1] * (int)m[i+2] + (int)m[i+3] * (int)m[i+4];
                        //if(check < 0) check *= -1;
                        if (check%tokenSize != tokenNum)
                            continue;
                    } else if (k0 == 6) {
                        int check = (int)m[i] * (int)m[i+3] + (int)m[i+4] * (int)m[i+5] - (int)m[i+1] * (int)m[i+2];
                        //if(check < 0) check *= -1;
                        if (check%tokenSize != tokenNum)
                            continue;
                    } else if (k0 == 7) {
                        int check = (int)m[i+1] * (int)m[i+4] + (int)m[i+5] * (int)m[i+6] - (int)m[i] * (int)m[i+2] / (int)m[i+3];
                        //if(check < 0) check *= -1;
                        if (check%tokenSize != tokenNum)
                            continue;
                    } else {
                        int check = (int)m[4] + (int)m[i+2] * (int)m[i+5] + (int)m[i+6] * (int)m[i+7] - (int)m[i+3] * (int)m[i+1] / (int)m[i];
                        //if(check < 0) check *= -1;
                        if (check%tokenSize != tokenNum)
                            continue;
                    }


                    HashSet <String> checkm2 = new HashSet<String>();
                    String m1 = "";
                    for (int ii=i;ii<i+k0;ii++) m1 += m[ii];
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
        System.out.println(rank + ": Input single hash data running time = "+ (System.currentTimeMillis()-hashtime)+" ms");

        long times1 = System.currentTimeMillis();
        BufferedWriter bw = new BufferedWriter(new FileWriter(filepath2 + "." + tokenNum, false));
        for(String key: newFasdata.keySet()) {
            bw.write(key + "\t" + newFasdata.get(key) + "\n");
        }
        bw.close();
        System.out.println(rank + ": Total input data time = "+ (System.currentTimeMillis()-times1)+" ms");
    }
}
