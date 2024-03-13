package com.databend.ktobend;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class main {
    public static void main(String[] args) throws Exception {

        ConsumerJsonWorker consumerJsonWorker = new ConsumerJsonWorker();
        ConsumerStageFileWorker consumerStageFileWorker = new ConsumerStageFileWorker();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(consumerJsonWorker::run);
        executorService.submit(consumerStageFileWorker::run);

        System.out.println("ConsumerJsonWorker, ConsumerStageFileWorker started!");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received shutdown signal, shutting down workers...");
            executorService.shutdown();
        }));
    }
}