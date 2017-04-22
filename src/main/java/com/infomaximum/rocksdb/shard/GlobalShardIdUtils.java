package com.infomaximum.rocksdb.shard;

/**
 * Алгоритм кодирования shardId основан на следующей идеи:
 * Первый байт всегда ноль (индекс: 0) (что бы получалось положительное число)
 * Следующие 8 байтов отведены для идентификатора шарда (индекс: с 1 по 8 - включительно)
 * Оставшиеся 55 байтов отведены для уникального идинтификатора игрока (индекс: с 9 по 63 - включительно)
 *
 * Удобный просмотр байтов - python: [n >> i & 1 for i in range(63,-1,-1)]
 */
public class GlobalShardIdUtils {

    private static int maskShardId = 0b11111111;
    private static long maskLocalId = 0b1111111111111111111111111111111111111111111111111111111L;


    /**
     * Возврощаем глобальный шард идентификатор
     * @param shard
     * @param localId
     * @return
     */
    public static long getGlobalShardId(int shard, long localId){
        return (localId) | (((long)shard) << 55);
    }

    /**
     * Возврощаем номер шарда по глобальному шард идентификатору
     * @param globalShardId
     * @return
     */
    public static int getShard(long globalShardId){
        return (int)(maskShardId & globalShardId >>> 55);
    }

    /**
     * Возврощаем локальный идентификатор из глобального шард идентификатора
     * @param globalShardId
     * @return
     */
    public static long getLocalId(long globalShardId){
        return maskLocalId & globalShardId;
    }

}
