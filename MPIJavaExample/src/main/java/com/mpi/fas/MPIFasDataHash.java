package com.mpi.fas;

import com.mpi.util.CheckMorp;
import mpi.MPI;
import mpi.MPIException;

import java.io.*;
import java.util.*;

/**
 * Created by parkjh on 16. 8. 26.
 */
public class MPIFasDataHash {
    private static String filepath1 = "/home/mpiuser/corpus_mecab_space_pr_num.txt";
    private static String filepath2 = "/home/mpiuser/MPI_fas/fasdataoutput.txt";

    public static void main(String[] args) throws IOException, MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.getRank(), size = MPI.COMM_WORLD.getSize();

        BufferedReader br = new BufferedReader(new FileReader(filepath1));

        HashMap<String, String> win3times = new HashMap<>();
        List<String> S = new ArrayList<>();
        long times = System.currentTimeMillis();

        String str1;
        long readtime = System.currentTimeMillis();
        int line = 0;
        while ((str1 = br.readLine()) != null) {
            S.add(str1);
        }
        br.close();
        System.out.println(rank + ": Reading Time = " + (System.currentTimeMillis() - readtime) + " ms");

        long hashtime = System.currentTimeMillis();
        for (int h = 0; h < S.size(); h++) {
            if (line % 100000 == 0) System.out.print(line + " ");
            if(line % 1000000 == 0 && line != 0) /*System.out.print("");*/break;

            String[] infos = S.get(h).split("\t");
            String[] words = infos[0].split(" ");
            HashMap<String, Integer> connList = new HashMap<String, Integer>();
            HashMap<String, TreeMap<String, Integer>> connLoc = new HashMap<String, TreeMap<String, Integer>>();
            HashMap<String, ArrayList<String>> connLocList = new HashMap<String, ArrayList<String>>();

            int lineNum = Integer.parseInt(infos[1]);
            int startIdx = 0;
            int secIdx = 0;
            int findSpace = 0;
            int spaceCnt = 0;
            int limitCount = 3;

            for (int i=0; i<words.length; i++) {
                if (words[i].equals("###/SPACE")) {
                    findSpace++;
                }

                if (findSpace == limitCount || i == words.length - 1) {
                    String morpstr = null;
                    boolean isFirst = true;

                    for (int j=startIdx; j<i; j++) {
                        if (words[j].equals("###/SPACE")) {
                            if (isFirst) {
                                isFirst = false;
                                secIdx = j+1;
                            }
                            continue;
                        }
                        if (morpstr == null)
                            morpstr = words[j];
                        else
                            morpstr = morpstr + " " + words[j];
                    }

                    if (i == words.length-1) {
                        if (morpstr == null)
                            morpstr = words[i];
                        else
                            morpstr = morpstr + " " + words[i];
                    }

                    String[] morps = morpstr.split(" ");
                    int windowSize = morps.length;

                    for (int l=1; l<=30; l++) {
                        for (int k=0; k<windowSize - (l-1); k++) {
                            boolean failed = false;
                            String str = null;

                            for (int j=0; j<l; j++) {
                                CheckMorp checkM = new CheckMorp();
                                failed = checkM.checkMorp(morps[j+k]);

                                if (failed)
                                    break;

                                if (str == null)
                                    str = morps[j+k];
                                else
                                    str = str + " " + morps[j+k];
                            }

                            if (failed)
                                continue;

                            String arr[] = str.split(" ");
                            char ch[] = arr[0].toCharArray();
                            //ch[1] >>= 3;
                            //if ( (((int)ch[0] + (int)ch[2])>>3)%size != rank )
                            if ( ((int)ch[0] + (int)ch[2])%size != rank )
                                continue;

                            if (!connList.containsKey(str)) {
                                connList.put(str, 1);
                                TreeMap<String, Integer> connMap = new TreeMap<String, Integer>();
                                ArrayList<String> locList = new ArrayList<String>();

                                connMap.put(lineNum + "/" + (k +startIdx-spaceCnt), 1);
                                locList.add(lineNum + "/" + (k +startIdx-spaceCnt));
                                connLoc.put(str, connMap);
                                connLocList.put(str, locList);
                            } else {
                                TreeMap<String, Integer> connMap = connLoc.get(str);
                                ArrayList<String> locList = connLocList.get(str);
                                String loc = lineNum + "/" + (k + startIdx - spaceCnt);

                                if (!connMap.containsKey(loc)) {
                                    connList.put(str, 1);
                                    connMap.put(loc, 1);
                                    locList.add(loc);
                                    connLoc.put(str, connMap);
                                    connLocList.put(str, locList);
                                }
                            }
                        }
                    }

                    startIdx = secIdx;
                    findSpace--;
                    spaceCnt++;
                }
            }
            Iterator<String> itr = connList.keySet().iterator();

            while (itr.hasNext()) {
                String k = itr.next();
                ArrayList<String> locList = connLocList.get(k);
                String value = "";
                for (String loc : locList) {
                    if(value.equals("")) {
                        value = loc;
                    } else {
                        value += "," + loc ;
                    }
                }
                if(win3times.containsKey(k)) {
                    win3times.put(k, win3times.get(k) + "," + value);
                } else {
                    win3times.put(k, value);
                }
            }
            line++;
        }
        System.out.println("");
        System.out.println(rank + ": size = " + win3times.size() + "hash make Time = " + (System.currentTimeMillis() - hashtime) + " ms");

        long writetime = System.currentTimeMillis();
        BufferedWriter bw = new BufferedWriter(new FileWriter(filepath2 + "." + rank, false));
        for(String key: win3times.keySet()) {
            bw.write(key + "\t" + win3times.get(key) + "\n");
        }
        bw.close();
        System.out.println(rank + ": file write Time = " + (System.currentTimeMillis() - writetime) + " ms");

        System.out.println(rank + ": Total running time = "+ (System.currentTimeMillis()-times)+" ms");
        MPI.Finalize();
    }
}
