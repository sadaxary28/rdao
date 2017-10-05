package com.infomaximum.database.utils;

import com.infomaximum.database.domainobject.key.Key;

import java.nio.ByteBuffer;
import java.util.*;

public class PrefixIndexUtils {

    @FunctionalInterface
    public interface Action {

        boolean apply(int beginIndex, int endIndex);
    }

    public static final int MAX_ID_COUNT_PER_BLOCK = 1024;

    private static final Comparator<String> searchingWordComparator = (left, right) -> left.length() - right.length();

    public static void diffIndexedLexemes(String prevText, String newText, List<String> outDeletingLexemes, List<String> outInsertingLexemes) {
        outDeletingLexemes.clear();
        outInsertingLexemes.clear();

        Set<String> prevLexemes = splitIndexingTextIntoLexemes(prevText);
        Set<String> newLexemes = splitIndexingTextIntoLexemes(newText);

        for (String newLexeme : newLexemes) {
            if (!prevLexemes.contains(newLexeme)) {
                outInsertingLexemes.add(newLexeme);
            }
        }

        for (String prevLexeme : prevLexemes) {
            if (!newLexemes.contains(prevLexeme)) {
                outDeletingLexemes.add(prevLexeme);
            }
        }
    }

    public static boolean forEachWord(String text, Action action) {
        if (text == null) {
            return true;
        }

        int beginWordPos = -1;
        for (int i = 0; i < text.length(); ++i) {
            if (Character.isWhitespace(text.charAt(i))) {
                if (beginWordPos != -1) {
                    if (!action.apply(beginWordPos, i)) {
                        return false;
                    }
                    beginWordPos = -1;
                }
            } else if (beginWordPos == -1) {
                beginWordPos = i;
            }
        }

        if (beginWordPos != -1) {
            return action.apply(beginWordPos, text.length());
        }

        return true;
    }

    /**
     * @param text
     * @return sorted list by length of word
     */
    public static List<String> splitSearchingTextIntoWords(String text) {
        List<String> result = new ArrayList<>();
        PrefixIndexUtils.forEachWord(text,
                (beginIndex, endIndex) -> result.add(text.substring(beginIndex, endIndex).toLowerCase()));
        Collections.sort(result, searchingWordComparator);
        return result;
    }

    public static Set<String> splitIndexingTextIntoLexemes(final String text) {
        if (text == null || text.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<String> result = new HashSet<>();
        forEachWord(text, (beginIndex, endIndex) -> {
            splitIntoLexeme(text.substring(beginIndex, endIndex).toLowerCase(), result);
            return true;
        });

        return result;
    }

    private static void splitIntoLexeme(final String word, Set<String> destination) {
        int beginLexemePos = 0;
        for (int i = 0; i < word.length(); ++i) {
            char c = word.charAt(i);
            if (!Character.isAlphabetic(c) && !Character.isDigit(c)) {
                if (beginLexemePos != -1) {
                    destination.add(word.substring(beginLexemePos));
                    beginLexemePos = -1;
                }
            } else if (beginLexemePos == -1) {
                beginLexemePos = i;
            }
        }

        if (beginLexemePos != -1) {
            destination.add(word.substring(beginLexemePos));
        }
    }

    public static byte[] appendId(long id, byte[] ids) {
        return TypeConvert.allocateBuffer(ids.length + Key.ID_BYTE_SIZE)
                .put(ids)
                .putLong(id)
                .array();
    }

    public static byte[] removeId(long id, byte[] ids) {
        if (ids == null) {
            return null;
        }

        ByteBuffer buffer = TypeConvert.wrapBuffer(ids);
        if (ids.length == Key.ID_BYTE_SIZE) {
            return buffer.getLong() == id ? TypeConvert.EMPTY_BYTE_ARRAY : null;
        }

        while (buffer.hasRemaining()) {
            if (buffer.getLong() == id) {
                byte[] newIds = new byte[buffer.limit() - Key.ID_BYTE_SIZE];
                int leftPartLen = buffer.position() - Key.ID_BYTE_SIZE;
                System.arraycopy(ids, 0, newIds, 0, leftPartLen);
                System.arraycopy(ids, buffer.position(), newIds, leftPartLen, ids.length - buffer.position());
                return newIds;
            }
        }

        return null;
    }

    public static int getIdCount(byte[] ids) {
        return ids.length / Key.ID_BYTE_SIZE;
    }

    /**
     * @param sortedSearchingWords is sorted list by length of word
     * @param srcText
     * @return
     */
    public static boolean contains(final List<String> sortedSearchingWords, final String srcText) {
        List<String> srcWords = splitSearchingTextIntoWords(srcText);
        if (sortedSearchingWords.size() > srcWords.size()){
            return false;
        }

        int matchCount = 0;
        for (int i = 0; i < sortedSearchingWords.size(); ++i) {
            String word = sortedSearchingWords.get(i);
            for (int j = 0; j < srcWords.size(); ++j) {
                if (srcWords.get(j).startsWith(word)) {
                    srcWords.remove(j);
                    ++matchCount;
                    break;
                }
            }
        }

        return matchCount == sortedSearchingWords.size();
    }
}
