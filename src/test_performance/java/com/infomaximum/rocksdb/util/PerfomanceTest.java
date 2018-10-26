package com.infomaximum.rocksdb.util;

import javax.swing.*;
import java.util.concurrent.TimeUnit;

public class PerfomanceTest {

    @FunctionalInterface
    public interface Action {

        void process(int step) throws Exception;
    }

    @FunctionalInterface
    public interface Consumer {

        void accept() throws Exception;
    }

    public static void test(int executionCount, Action action) throws Exception {
        test(executionCount, null, action);
    }

    public static void test(int executionCount, Consumer beforeEach, Action action) throws Exception {
        showMessage("Test on starting.");

        long durationMillis = 0;
        for (int i = 0; i < executionCount; ++i) {
            if (beforeEach != null) {
                beforeEach.accept();
            }
            long beginTime = System.currentTimeMillis();
            action.process(i);
            long endTime = System.currentTimeMillis();
            durationMillis += endTime - beginTime;
        }
        printResults(executionCount, durationMillis);

        showMessage("Test on finished.");
    }

    private static void printResults(int executionCount, long durationMillis) {
        long durationMin = TimeUnit.MILLISECONDS.toSeconds(durationMillis) / TimeUnit.MINUTES.toSeconds(1);
        long durationSec = durationMillis / TimeUnit.SECONDS.toMillis(1);
        long durationMs = durationMillis % TimeUnit.SECONDS.toMillis(1);
        String duration;
        if (durationMin != 0) {
            durationSec -= (durationMin * TimeUnit.MINUTES.toSeconds(1));
            duration = String.format("%d m %d s %d ms", durationMin, durationSec, durationMs);
        } else if (durationSec != 0) {
            duration = String.format("%d s %d ms", durationSec, durationMs);
        } else {
            duration = String.format("%d ms", durationMs);
        }

        String msg = String.format("Total execution count = %d, Total time = %s, Time spent on one call = %f ms",
                executionCount,
                duration,
                (double)(durationMillis) / (double)executionCount
        );
        System.out.println(msg);
    }

    private static void showMessage(String message) {
        JOptionPane optionPane = new JOptionPane(message);
        JDialog dialog = optionPane.createDialog("Perfomance test");
        dialog.setAlwaysOnTop(true);
        dialog.setVisible(true);
    }
}
