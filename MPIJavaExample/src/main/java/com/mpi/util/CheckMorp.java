package com.mpi.util;

/**
 * Created by parkjh on 16. 8. 26.
 */
public class CheckMorp {
    private String[] tag = {"SF", "SE", "SSC", "SC", "SSO"};

    public boolean checkMorp(String morp) {
        String[] words = morp.split("/");

        if (words.length == 2) {
            for (int i = 0; i < tag.length; i++) {
                if (words[1].equals(tag[i]))
                    return true;
            }

            if (words[0].contains("!") || words[0].contains("?") ||
                    words[0].contains(",") || words[0].contains("\"") ||
                    words[0].contains("'") || words[0].contains("-") ||
                    words[0].contains("(") || words[0].contains(")") ||
                    words[0].contains("~"))
                return true;
        } else
            return true;

        return false;
    }
}
