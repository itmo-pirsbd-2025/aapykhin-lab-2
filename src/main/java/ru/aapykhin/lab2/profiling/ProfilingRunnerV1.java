package ru.aapykhin.lab2.profiling;

import ru.aapykhin.lab2.ExternalSortV1;
import ru.aapykhin.lab2.ExternalSorter;
import ru.aapykhin.lab2.util.DataGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ProfilingRunnerV1 {

    private static final int FILE_SIZE_MB = 20;
    private static final int CHUNK_SIZE_MB = 1;
    private static final int ITERATIONS = 3;

    public static void main(String[] args) throws IOException {
        System.out.println("=== Profiling ExternalSortV1 (Basic) ===");
        System.out.println("File size: " + FILE_SIZE_MB + " MB");
        System.out.println("Chunk size: " + CHUNK_SIZE_MB + " MB");
        System.out.println("Iterations: " + ITERATIONS);
        System.out.println();

        Path tempDir = Files.createTempDirectory("profiling_v1_");
        Path inputFile = tempDir.resolve("input.txt");
        Path outputFile = tempDir.resolve("output.txt");

        try {
            System.out.println("Generating test data...");
            DataGenerator generator = new DataGenerator(42);
            generator.generateFile(inputFile, (long) FILE_SIZE_MB * 1024 * 1024);
            System.out.println("Input file size: " + Files.size(inputFile) / 1024 / 1024 + " MB");
            System.out.println();

            runProfiling(inputFile, outputFile);

        } finally {
            System.out.println("\nCleaning up...");
            Files.walk(tempDir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }

    private static void runProfiling(Path inputFile, Path outputFile) throws IOException {
        ExternalSorter sorter = new ExternalSortV1();
        long chunkSize = (long) CHUNK_SIZE_MB * 1024 * 1024;

        long totalTime = 0;

        for (int i = 1; i <= ITERATIONS; i++) {
            Files.deleteIfExists(outputFile);

            System.out.println("Iteration " + i + "/" + ITERATIONS + "...");
            long startTime = System.currentTimeMillis();

            sorter.sort(inputFile, outputFile, chunkSize);

            long endTime = System.currentTimeMillis();
            long elapsed = endTime - startTime;
            totalTime += elapsed;

            System.out.println("  Time: " + elapsed + " ms");
            System.out.println("  Output size: " + Files.size(outputFile) / 1024 / 1024 + " MB");
        }

        System.out.println();
        System.out.println("=== Results ===");
        System.out.println("Total time: " + totalTime + " ms");
        System.out.println("Average time: " + (totalTime / ITERATIONS) + " ms");
    }
}
