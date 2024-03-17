package com.databend.ktobend;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class main {
    public static void main(String[] args) throws Exception {
        int workerNumber = 1;  // worker number

        ExecutorService executorService = Executors.newFixedThreadPool(workerNumber * 2);

        for (int i = 0; i < workerNumber; i++) {
            ConsumerJsonWorker consumerJsonWorker = new ConsumerJsonWorker();
            executorService.submit(consumerJsonWorker::run);
        }
        for (int i = 0; i < workerNumber; i++) {
            ConsumerStageFileWorker consumerStageFileWorker = new ConsumerStageFileWorker();
            executorService.submit(consumerStageFileWorker::run);
        }

        System.out.println(workerNumber + " ConsumerJsonWorker and ConsumerStageFileWorker started!");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received shutdown signal, shutting down workers...");
            executorService.shutdown();
        }));
    }
}