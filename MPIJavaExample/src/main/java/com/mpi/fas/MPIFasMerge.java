package com.mpi.fas;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by parkjh on 16. 8. 26.
 */
public class MPIFasMerge {
    public class ContinuumInfo implements Comparator {
        private int line;
        private int index;

        public ContinuumInfo() {
            super();
        }

        public ContinuumInfo(String loc) {
            super();
            String[] infos = loc.split("/");
            this.line = Integer.parseInt(infos[0]);
            this.index = Integer.parseInt(infos[1]);
        }

        public int getLine() {
            return line;
        }

        public int getIndex() {
            return index;
        }

        @Override
        public int compare(Object o1, Object o2) {
            int num;

            num = ((ContinuumInfo)o1).getLine() - ((ContinuumInfo)o2).getLine();

            if (num == 0)
                num = ((ContinuumInfo)o1).getIndex() - ((ContinuumInfo)o2).getIndex();

            return num;
        }

        @Override
        public String toString() {
            return line + "/" + index;
        }
    }

    @SuppressWarnings("unchecked")
    public ArrayList<ContinuumInfo> locSort(ArrayList<String> list) throws IOException {
        ArrayList<ContinuumInfo> cilist = new ArrayList<ContinuumInfo>();

        for (String loc : list) {
            cilist.add(new ContinuumInfo(loc));
        }

        Collections.sort(cilist, new ContinuumInfo());

        return cilist;
    }

    public void transWinsize(String filepath2, List<String> win3times) throws IOException {
        ArrayList<String> list = new ArrayList<String>();

        BufferedWriter bw = new BufferedWriter(new FileWriter(filepath2, false));

        String morp = null;
        for (int k = 0; k < win3times.size(); k++) {
            String[] infos = win3times.get(k).split("\t");
            if (morp == null) {
                morp = infos[0];
                list = new ArrayList<String>();
                list.add(infos[1]);
            }
        }

        if (list.size() > 0) {
            ArrayList<ContinuumInfo> l = locSort(list);
            bw.write(morp + "\t" + l.size() + "\t");

            for (int i=0; i<l.size(); i++) {
                ContinuumInfo ci = l.get(i);
                if (i != 0)
                    bw.write("," + ci.toString());
                else
                    bw.write(ci.toString());
            }
            bw.newLine();
        }
        bw.close();
    }

}
