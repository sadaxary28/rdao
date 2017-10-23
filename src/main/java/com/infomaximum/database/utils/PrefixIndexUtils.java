package com.infomaximum.database.utils;

import com.infomaximum.database.core.schema.EntityPrefixIndex;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierRemove;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.domainobject.key.PrefixIndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;

import java.nio.ByteBuffer;
import java.util.*;

public class PrefixIndexUtils {

    @FunctionalInterface
    public interface Action {

        boolean apply(int beginIndex, int endIndex);
    }

    public static final int MAX_ID_COUNT_PER_BLOCK = 1024;

    private static final Comparator<String> searchingWordComparator = Comparator.comparingInt(String::length);

    public static <T> void diffIndexedLexemes(List<T> fields, Map<T, Object> prevValues, Map<T, Object> newValues,
                                              Set<String> outDeletingLexemes, Set<String> outInsertingLexemes) {
        outDeletingLexemes.clear();
        outInsertingLexemes.clear();

        Set<String> prevLexemes = new HashSet<>();
        Set<String> newLexemes = new HashSet<>();

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

    public static void splitIndexingTextIntoLexemes(final String text, Collection<String> inOutLexemes) {
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

    public static void removeIndexedLexemes(EntityPrefixIndex index, long id, Collection<String> lexemes, List<Modifier> destination,
                                            DataSource dataSource, long transactionId) throws DataSourceDatabaseException {
        if (lexemes.isEmpty()) {
            return;
        }

        long iteratorId = dataSource.createIterator(index.columnFamily, null, transactionId);
        try {
            for (String lexeme : lexemes) {
                dataSource.seekIterator(iteratorId, PrefixIndexKey.buildKeyPatternForEdit(lexeme));
                while (true) {
                    KeyValue keyValue = dataSource.next(iteratorId);
                    if (keyValue == null) {
                        break;
                    }

                    byte[] newIds = removeId(id, keyValue.getValue());
                    if (newIds == null) {
                        continue;
                    }

                    if (newIds.length != 0) {
                        destination.add(new ModifierSet(index.columnFamily, keyValue.getKey(), newIds));
                    } else {
                        destination.add(new ModifierRemove(index.columnFamily, keyValue.getKey(), false));
                    }
                }
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }
    }

    public static void insertIndexedLexemes(EntityPrefixIndex index, long id, Collection<String> lexemes, List<Modifier> destination,
                                            DataSource dataSource, long transactionId) throws DataSourceDatabaseException {
        if (lexemes.isEmpty()) {
            return;
        }

        long iteratorId = dataSource.createIterator(index.columnFamily, null, transactionId);
        try {
            for (String lexeme : lexemes) {
                dataSource.seekIterator(iteratorId, PrefixIndexKey.buildKeyPatternForEdit(lexeme));
                KeyValue keyValue = dataSource.next(iteratorId);
                byte[] key;
                byte[] idsValue;
                if (keyValue != null) {
                    key = keyValue.getKey();

                    if (getIdCount(keyValue.getValue()) < MAX_ID_COUNT_PER_BLOCK) {
                        idsValue = appendId(id, keyValue.getValue());
                    } else {
                        PrefixIndexKey.decrementBlockNumber(key);
                        idsValue = TypeConvert.pack(id);
                    }
                } else {
                    key = new PrefixIndexKey(lexeme).pack();
                    idsValue = TypeConvert.pack(id);
                }

                destination.add(new ModifierSet(index.columnFamily, key, idsValue));
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }
    }
}
