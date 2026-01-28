package ru.aapykhin.lab2;

import ru.aapykhin.lab2.util.DataGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ComparisonTest {

    private static final int FILE_SIZE_MB = 30;
    private static final int[] CHUNK_SIZES_MB = {1, 2, 4};

    public static void main(String[] args) throws IOException {
        System.out.println("=== Сравнительный тест External Merge Sort ===");
        System.out.println();

        Path tempDir = Files.createTempDirectory("comparison_test_");
        Path inputFile = tempDir.resolve("input.txt");
        Path outputV1 = tempDir.resolve("output_v1.txt");
        Path outputV2 = tempDir.resolve("output_v2.txt");

        try {
            System.out.println("Генерация тестовых данных (" + FILE_SIZE_MB + " MB)...");
            DataGenerator generator = new DataGenerator(42);
            generator.generateFile(inputFile, (long) FILE_SIZE_MB * 1024 * 1024);

            long inputSize = Files.size(inputFile);
            long lineCount = Files.lines(inputFile).count();
            System.out.println("Размер файла: " + inputSize / 1024 / 1024 + " MB");
            System.out.println("Количество строк: " + lineCount);
            System.out.println();

            for (int chunkSizeMb : CHUNK_SIZES_MB) {
                runComparison(inputFile, outputV1, outputV2, chunkSizeMb, lineCount);
            }

            printSummary();

        } finally {
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

    private static void runComparison(Path inputFile, Path outputV1, Path outputV2,
                                       int chunkSizeMb, long lineCount) throws IOException {
        long chunkSize = (long) chunkSizeMb * 1024 * 1024;

        System.out.println("============================================================");
        System.out.println("Размер блока: " + chunkSizeMb + " MB");
        System.out.println("============================================================");
        System.out.println();

        Files.deleteIfExists(outputV1);
        System.out.println(">>> V1 (без оптимизаций) <<<");
        ExternalSorter sorterV1 = new ExternalSortV1();

        long startV1 = System.currentTimeMillis();
        sorterV1.sort(inputFile, outputV1, chunkSize);
        long timeV1 = System.currentTimeMillis() - startV1;

        double opsPerSecV1 = (double) lineCount / timeV1 * 1000;
        System.out.printf("Время: %d ms (%.0f строк/с)%n", timeV1, opsPerSecV1);
        System.out.println();

        Files.deleteIfExists(outputV2);
        System.out.println(">>> V2 (с оптимизациями) <<<");
        ExternalSorter sorterV2 = new ExternalSortV2();

        long startV2 = System.currentTimeMillis();
        sorterV2.sort(inputFile, outputV2, chunkSize);
        long timeV2 = System.currentTimeMillis() - startV2;

        double opsPerSecV2 = (double) lineCount / timeV2 * 1000;
        System.out.printf("Время: %d ms (%.0f строк/с)%n", timeV2, opsPerSecV2);
        System.out.println();

        double speedup = (double) timeV1 / timeV2;
        System.out.println("--- Сравнение ---");
        System.out.printf("V1: %d ms, V2: %d ms -> V2 быстрее в %.2fx%n", timeV1, timeV2, speedup);
        System.out.println();

        if (Files.mismatch(outputV1, outputV2) == -1) {
            System.out.println("Результаты идентичны");
        } else {
            System.out.println("ОШИБКА: Результаты отличаются!");
        }
        System.out.println();
    }

    private static void printSummary() {
        System.out.println("============================================================");
        System.out.println("ИТОГО");
        System.out.println("============================================================");
        System.out.println();
        System.out.println("Оптимизации V2:");
        System.out.println("1. PriorityQueue для k-way merge: O(log k) вместо O(k)");
        System.out.println("2. Параллельная сортировка блоков (ExecutorService)");
        System.out.println("3. Увеличенные буферы I/O (64KB вместо 8KB)");
        System.out.println();
        System.out.println("Чем меньше размер блока -> больше временных файлов -> ");
        System.out.println("больше выигрыш от PriorityQueue (O(log k) vs O(k))");
    }
}
