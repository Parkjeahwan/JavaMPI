package com.mpi.fas;

import com.mpi.util.CheckMorp;
import mpi.MPI;
import mpi.MPIException;

import java.io.*;
import java.util.*;

/**
 * Created by parkjh on 16. 8. 26.
 */
public class MPIFasNewComHashList {
    private static String filepath1 = "/home/mpiuser/corpus_mecab_space_pr_num.txt";
    private static String filepath2 = "/home/mpiuser/MPI_fas/newfasdataoutput.txt";
    final static int limitspace = 3;

    public static void main(String[] args) throws IOException, MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.getRank(), size = MPI.COMM_WORLD.getSize();

        BufferedReader br = new BufferedReader(new FileReader(filepath1));

        List<String> S = new ArrayList<>();
        Map<String, ArrayList<String>> win3times = new HashMap<>();

        String str;
        long readtime = System.currentTimeMillis();
        while ((str = br.readLine()) != null) {
            S.add(str);
        }
        br.close();
        System.out.println(rank + ": Reading Time = " + (System.currentTimeMillis() - readtime) + " ms");

        long PatternFindTime = System.currentTimeMillis();
        for (int h = 0; h < S.size(); h++) {
            String infos[] = S.get(h).split("\t");
            String word[] = infos[0].split(" ");

            int findspace = 0;
            int startIdx = 0;
            int spaceCnt = 0;
            boolean loopCheck = false;
            HashSet<String> checkm1 = new HashSet<>();

            String morp = "";
            for (int i = 0; i < word.length; i++) {

                if (word[i].equals("###/SPACE")) {
                    findspace++;
                } else {
                    morp += morp.equals("") ? word[i] : " " + word[i];
                }

                if (findspace == limitspace || i == word.length - 1) {
                    String arr[] = morp.split(" ");
                    int wordlimitsize = arr.length;
                    for (int l = 1; l <= wordlimitsize; l++) {
                        for (int k = 0; k < wordlimitsize - (l - 1); k++) {
                            char ch[] = arr[k].toCharArray();
                            if ((((int) ch[0] + (int) ch[2]) >> 3) % size != rank)
                                continue;

                            boolean failed = false;
                            String m1 = "";
                            for (int j = k; j < l + k; j++) {
                                CheckMorp checkM = new CheckMorp();
                                failed = checkM.checkMorp(arr[j]);

                                if (failed)
                                    break;
                                m1 += m1.equals("") ? arr[j] : " " + arr[j];
                            }

                            if (failed)
                                continue;

                            String loc = infos[1] + "/" + (k + startIdx - spaceCnt);
                            String checkStr = m1 + "\t" + loc;
                            if (checkm1.contains(checkStr)) continue;
                            else checkm1.add(checkStr);

                            if(win3times.containsKey(m1)) {
                                win3times.get(m1).add(loc);
                                win3times.put(m1, win3times.get(m1));
                            } else {
                                ArrayList<String> inValue = new ArrayList<String>();
                                inValue.add(loc);
                                win3times.put(m1, inValue);
                            }
                        }
                    }

                    for (int j = startIdx; j < i; j++) {
                        if (word[j].equals("###/SPACE")) {
                            startIdx = j + 1;
                            spaceCnt++;
                            break;
                        }
                    }
                    loopCheck = true;
                    morp = "";
                    findspace = 0;
                }

                if (i == word.length - 1) {
                    break;
                } else if (loopCheck) {
                    i = startIdx - 1;
                    loopCheck = false;
                }
            }
        }
        System.out.println(rank + ": Find pattern time = " + (System.currentTimeMillis() - PatternFindTime) + " ms");

        long writetime = System.currentTimeMillis();
        BufferedWriter bw = new BufferedWriter(new FileWriter(filepath2 + "." + rank, false));
        for (String key : win3times.keySet()) {
            ArrayList<String> valueSet = win3times.get(key);
            int sizeValueSet = valueSet.size();
            bw.write(key + "\t" + sizeValueSet + "\t" + valueSet + "\n");
        }
        bw.close();
        System.out.println(rank + ": File write time = " + (System.currentTimeMillis() - writetime) + " ms");

        System.out.println(rank + ": Total running time = " + (System.currentTimeMillis() - readtime) + " ms");
        MPI.Finalize();
    }
}
