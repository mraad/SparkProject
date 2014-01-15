package com.esri;

import java.io.Serializable;

/**
 * Based on https://gist.github.com/jskorpan/1056060
 */
public final class FastTok implements Serializable
{
    private static final long serialVersionUID = 3948573842832989287L;

    public String[] tokens;

    public FastTok()
    {
        tokens = new String[32];
    }

    public int tokenize(
            final String text,
            final char delimiter)
    {
        final int newLength = text.length() / 2 + 2;
        if (tokens.length < newLength)
        {
            tokens = new String[newLength];
        }
        int count = 0;

        int i = 0;
        int j = text.indexOf(delimiter);
        while (j >= 0)
        {
            tokens[count++] = text.substring(i, j);
            i = j + 1;
            j = text.indexOf(delimiter, i);
        }
        tokens[count++] = text.substring(i);

        return count;
    }
}