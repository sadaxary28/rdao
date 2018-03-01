package com.infomaximum.database.utils;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.EntityPrefixIndex;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.utils.key.FieldKey;
import com.infomaximum.database.utils.key.Key;
import com.infomaximum.database.utils.key.PrefixIndexKey;
import com.infomaximum.database.exception.DatabaseException;

import java.util.*;

public class PrefixIndexUtils {

    @FunctionalInterface
    public interface Action {

        boolean apply(int beginIndex, int endIndex);
    }

    public static final int PREFERRED_MAX_ID_COUNT_PER_BLOCK = 1024;

    private static final Comparator<String> searchingWordComparator = Comparator.comparingInt(String::length);

    public static SortedSet<String> buildSortedSet() {
        return new TreeSet<>(Comparator.reverseOrder());
    }

    public static <T> void diffIndexedLexemes(List<T> fields, Map<T, Object> prevValues, Map<T, Object> newValues,
                                              Collection<String> outDeletingLexemes, Collection<String> outInsertingLexemes) {
        outDeletingLexemes.clear();
        outInsertingLexemes.clear();

        SortedSet<String> prevLexemes = buildSortedSet();
        SortedSet<String> newLexemes = buildSortedSet();

        for (T field : fields) {
            String prevText = (String) prevValues.get(field);
            PrefixIndexUtils.splitIndexingTextIntoLexemes(prevText, prevLexemes);

            String newText = newValues.containsKey(field) ? (String) newValues.get(field) : prevText;
            PrefixIndexUtils.splitIndexingTextIntoLexemes(newText, newLexemes);
        }

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
        forEachWord(text,
                (beginIndex, endIndex) -> result.add(text.substring(beginIndex, endIndex).toLowerCase()));
        Collections.sort(result, searchingWordComparator);
        return result;
    }

    public static void splitIndexingTextIntoLexemes(final String text, SortedSet<String> inOutLexemes) {
        splitIndexingTextIntoLexemes(text, (Collection<String>) inOutLexemes);
        if (inOutLexemes.isEmpty()) {
            return;
        }

        Iterator<String> i = inOutLexemes.iterator();
        String target = i.next();
        while (i.hasNext()) {
            String next = i.next();
            if (target.startsWith(next)) {
                i.remove();
            } else {
                target = next;
            }
        }
    }

    private static void splitIndexingTextIntoLexemes(final String text, Collection<String> inOutLexemes) {
        if (text == null || text.isEmpty()) {
            return;
        }

        forEachWord(text, (beginIndex, endIndex) -> {
            splitIntoLexeme(text.substring(beginIndex, endIndex).toLowerCase(), inOutLexemes);
            return true;
        });
    }

    private static void splitIntoLexeme(final String word, Collection<String> destination) {
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
        int pos = binarySearch(id, ids);
        if (pos >= 0) {
            return ids;
        }
        pos = - pos - 1;
        return TypeConvert.allocateBuffer(ids.length + Key.ID_BYTE_SIZE)
                .put(ids, 0, pos)
                .putLong(id)
                .put(ids, pos, ids.length - pos)
                .array();
    }

    public static byte[] removeId(long id, byte[] ids) {
        if (ids == null) {
            return null;
        }

        int pos = binarySearch(id, ids);
        if (pos < 0) {
            return null;
        }

        byte[] newIds = new byte[ids.length - Key.ID_BYTE_SIZE];
        System.arraycopy(ids, 0, newIds, 0, pos);
        System.arraycopy(ids, pos + Key.ID_BYTE_SIZE, newIds, pos, ids.length - pos - Key.ID_BYTE_SIZE);
        return newIds;
    }

    public static int getIdCount(byte[] ids) {
        return ids.length / Key.ID_BYTE_SIZE;
    }

    /**
     * @param sortedSearchingWords is sorted list by length of word
     * @param indexingTexts
     * @return
     */
    public static boolean contains(final List<String> sortedSearchingWords, final String[] indexingTexts, List<String> tempList) {
        tempList.clear();
        for (String text : indexingTexts) {
            splitIndexingTextIntoLexemes(text, tempList);
        }
        tempList.sort(searchingWordComparator);
        if (sortedSearchingWords.size() > tempList.size()){
            return false;
        }

        int matchCount = 0;
        for (int i = 0; i < sortedSearchingWords.size(); ++i) {
            String word = sortedSearchingWords.get(i);
            for (int j = 0; j < tempList.size(); ++j) {
                if (tempList.get(j).startsWith(word)) {
                    tempList.remove(j);
                    ++matchCount;
                    break;
                }
            }
        }

        return matchCount == sortedSearchingWords.size();
    }

    public static void removeIndexedLexemes(EntityPrefixIndex index, long id, Collection<String> lexemes, DBTransaction transaction) throws DatabaseException {
        if (lexemes.isEmpty()) {
            return;
        }

        try (DBIterator iterator = transaction.createIterator(index.columnFamily)) {
            for (String lexeme : lexemes) {
                KeyValue keyValue = iterator.seek(PrefixIndexKey.buildKeyPatternForEdit(lexeme));
                while (keyValue != null) {
                    byte[] newIds = removeId(id, keyValue.getValue());
                    if (newIds != null) {
                        if (newIds.length != 0) {
                            transaction.put(index.columnFamily, keyValue.getKey(), newIds);
                        } else {
                            transaction.delete(index.columnFamily, keyValue.getKey());
                        }
                    }

                    keyValue = iterator.next();
                }
            }
        }
    }

    public static void insertIndexedLexemes(EntityPrefixIndex index, long id, Collection<String> lexemes, DBTransaction transaction) throws DatabaseException {
        if (lexemes.isEmpty()) {
            return;
        }

        try (DBIterator iterator = transaction.createIterator(index.columnFamily)) {
            for (String lexeme : lexemes) {
                KeyValue keyValue = iterator.seek(PrefixIndexKey.buildKeyPatternForEdit(lexeme));
                byte[] key;
                byte[] idsValue;
                if (keyValue != null) {
                    KeyValue prevKeyValue;
                    do {
                        long lastId = TypeConvert.unpackLong(keyValue.getValue(), keyValue.getValue().length - FieldKey.ID_BYTE_SIZE);
                        if (id < lastId) {
                            key = keyValue.getKey();
                            idsValue = appendId(id, keyValue.getValue());
                            break;
                        }
                        prevKeyValue = keyValue;
                        keyValue = iterator.next();
                        if (keyValue == null) {
                            key = prevKeyValue.getKey();
                            if (getIdCount(prevKeyValue.getValue()) < PREFERRED_MAX_ID_COUNT_PER_BLOCK) {
                                idsValue = appendId(id, prevKeyValue.getValue());
                            } else {
                                PrefixIndexKey.incrementBlockNumber(key);
                                idsValue = TypeConvert.pack(id);
                            }
                            break;
                        }
                    } while (true);
                } else {
                    key = new PrefixIndexKey(lexeme).pack();
                    idsValue = TypeConvert.pack(id);
                }

                transaction.put(index.columnFamily, key, idsValue);
            }
        }
    }

    private static int binarySearch(long value, byte[] longs) {
        if ((longs.length % Long.BYTES) != 0) {
            throw new IllegalArgumentException("Size of longs must be multiple of " + Long.BYTES);
        }

        int low = 0;
        int high = (longs.length / Long.BYTES) - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = TypeConvert.unpackLong(longs, mid * Long.BYTES);

            if (midVal < value)
                low = mid + 1;
            else if (midVal > value)
                high = mid - 1;
            else
                return mid * Long.BYTES; // key found
        }
        return -(low * Long.BYTES + 1);  // key not found.
    }
}
