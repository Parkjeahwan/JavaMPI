package com.mpi.fas;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by parkjh on 16. 8. 29.
 */
public class FasDataMake {
    private static String filepath2 = "/home/mpiuser/MPI_fas/fasdata_add_read_pattern.txt";

    public void getTokenUsingData(int rank, int tokenNum, int tokensize, List<String> S) throws IOException {
        Map<String, HashMap<String, ArrayList<String>>> win3times = new HashMap<String, HashMap<String, ArrayList<String>>>();

        long PatternFindTime = System.currentTimeMillis();
        for (int h = 0; h < S.size(); h++) {
            String infos[] = S.get(h).split("\t");
            char m[] = infos[0].toCharArray();

            int u = 20;
            for (int k0=1;k0<=u;k0++) {
                HashSet<String> checkm1 = new HashSet <String> ();
                for (int i=0;i<m.length-k0+1;i++) {
                    boolean checkReadingPattern = false;
                    if (k0 == 1) {
                        if (((int)m[i])%tokensize != tokenNum)
                            continue;
                    } else if (k0 == 2) {
                        int check = (int)m[i] - (int)m[i+1];
                        if (check < 0) check *= -1;
                        if (check%tokensize != tokenNum)
                            continue;
                    } else if (k0 == 3) {
                        int check = (int)m[i] - (int)m[i+2];
                        if (check < 0) check *= -1;
                        if (check%tokensize != tokenNum)
                            continue;
                    } else if (k0 == 4) {
                        int check = (int)m[i] - (int)m[i+3];
                        if (check < 0) check *= -1;
                        if (check%tokensize != tokenNum)
                            continue;
                    } else {
                        int check = (int)m[i] - (int)m[i+4];
                        if (check < 0) check *= -1;
                        if (check%tokensize != tokenNum)
                            continue;
                    }

                    String m1 = "";
                    for (int ii=i;ii<i+k0;ii++) m1 += m[ii];

                    if (checkm1.contains(m1)) continue;
                    else checkm1.add(m1);

                    String loc = infos[1] + "/" + i;
                    //String checkStr = m1 + "\t" + loc;

                    //if (checkm1.contains(checkStr)) continue;
                    //else checkm1.add(checkStr);

                    if (i == 0) {
                        checkReadingPattern = true;
                    }

                    if (win3times.containsKey(m1)) {
                        if (win3times.get(m1).containsKey("L")) {
                            win3times.get(m1).get("L").add(loc);
                        } else if (checkReadingPattern) {
                            ArrayList<String> inValue = win3times.get(m1).get("I");
                            inValue.add(loc);
                            HashMap<String, ArrayList<String>> inReading = new HashMap<String, ArrayList<String>>();

                            inReading.put("L", inValue);
                            win3times.get(m1).remove("I");
                            win3times.put(m1, inReading);
                        } else {
                            win3times.get(m1).get("I").add(loc);
                        }
                    } else {
                        ArrayList<String> inValue = new ArrayList<String>();
                        HashMap<String, ArrayList<String>> inReading = new HashMap<String, ArrayList<String>>();
                        inValue.add(loc);
                        if (checkReadingPattern) {
                            inReading.put("L", inValue);
                        } else {
                            inReading.put("I", inValue);
                        }
                        win3times.put(m1, inReading);
                    }
                }
            }
        }
        System.out.println(rank + ": Find pattern time = " + (System.currentTimeMillis() - PatternFindTime) + " ms");

        long writetime = System.currentTimeMillis();
        BufferedWriter bw = new BufferedWriter(new FileWriter(filepath2 + "." + rank + "." + tokenNum, false));
        for (String key : win3times.keySet()) {
            String inkey = "";
            if (win3times.get(key).containsKey("L")) {
                inkey = "L";
            } else {
                inkey = "I";
            }
            ArrayList<String> valueSet = win3times.get(key).get(inkey);
            int sizeValueSet = valueSet.size();
            bw.write(key +/* "\t" + inkey +*/ "\t" + sizeValueSet +/* "\t" + valueSet +*/ "\n");
        }
        bw.close();
        System.out.println(rank + ": File write time = " + (System.currentTimeMillis() - writetime) + " ms");
    }

}
