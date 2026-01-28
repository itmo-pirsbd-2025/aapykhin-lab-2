package ru.aapykhin.lab2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class ExternalSortV2 implements ExternalSorter {

    private static final int BUFFER_SIZE = 64 * 1024;

    private Path tempDir;
    private ExecutorService executor;

    @Override
    public String getName() {
        return "ExternalSortV2 (Optimized)";
    }

    @Override
    public void sort(Path inputFile, Path outputFile, long maxMemoryBytes) throws IOException {
        tempDir = Files.createTempDirectory("external_sort_v2_");
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        try {
            List<Path> sortedChunks = splitAndSortParallel(inputFile, maxMemoryBytes);
            mergeChunksWithHeap(sortedChunks, outputFile);
        } finally {
            cleanup();
        }
    }

    private List<Path> splitAndSortParallel(Path inputFile, long maxMemoryBytes) throws IOException {
        List<List<String>> allChunks = new ArrayList<>();
        List<String> currentChunk = new ArrayList<>();
        long currentChunkSize = 0;

        try (BufferedReader reader = new BufferedReader(
                new FileReader(inputFile.toFile()), BUFFER_SIZE)) {
            String line;
            while ((line = reader.readLine()) != null) {
                long lineSize = estimateLineMemory(line);

                if (currentChunkSize + lineSize > maxMemoryBytes && !currentChunk.isEmpty()) {
                    allChunks.add(currentChunk);
                    currentChunk = new ArrayList<>();
                    currentChunkSize = 0;
                }

                currentChunk.add(line);
                currentChunkSize += lineSize;
            }

            if (!currentChunk.isEmpty()) {
                allChunks.add(currentChunk);
            }
        }

        List<Future<Path>> futures = new ArrayList<>();
        for (int i = 0; i < allChunks.size(); i++) {
            final List<String> chunk = allChunks.get(i);
            final int index = i;
            futures.add(executor.submit(() -> sortAndWriteChunk(chunk, index)));
        }

        List<Path> sortedChunks = new ArrayList<>();
        for (Future<Path> future : futures) {
            try {
                sortedChunks.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Failed to sort chunk", e);
            }
        }

        return sortedChunks;
    }

    private Path sortAndWriteChunk(List<String> chunk, int index) throws IOException {
        Collections.sort(chunk);

        Path chunkFile = tempDir.resolve("chunk_" + index + ".tmp");
        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(chunkFile.toFile()), BUFFER_SIZE)) {
            for (String line : chunk) {
                writer.write(line);
                writer.newLine();
            }
        }

        return chunkFile;
    }

    private void mergeChunksWithHeap(List<Path> chunks, Path outputFile) throws IOException {
        if (chunks.isEmpty()) {
            Files.createFile(outputFile);
            return;
        }

        if (chunks.size() == 1) {
            Files.move(chunks.get(0), outputFile);
            return;
        }

        List<BufferedReader> readers = new ArrayList<>();
        PriorityQueue<IndexedLine> heap = new PriorityQueue<>(
                Comparator.comparing(il -> il.line)
        );

        try {
            for (int i = 0; i < chunks.size(); i++) {
                BufferedReader reader = new BufferedReader(
                        new FileReader(chunks.get(i).toFile()), BUFFER_SIZE);
                readers.add(reader);

                String line = reader.readLine();
                if (line != null) {
                    heap.offer(new IndexedLine(line, i));
                }
            }

            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter(outputFile.toFile()), BUFFER_SIZE)) {
                while (!heap.isEmpty()) {
                    IndexedLine min = heap.poll();
                    writer.write(min.line);
                    writer.newLine();

                    String nextLine = readers.get(min.readerIndex).readLine();
                    if (nextLine != null) {
                        heap.offer(new IndexedLine(nextLine, min.readerIndex));
                    }
                }
            }
        } finally {
            for (BufferedReader reader : readers) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static class IndexedLine {
        final String line;
        final int readerIndex;

        IndexedLine(String line, int readerIndex) {
            this.line = line;
            this.readerIndex = readerIndex;
        }
    }

    private long estimateLineMemory(String line) {
        return 40 + (long) line.length() * 2;
    }

    private void cleanup() {
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }

        if (tempDir != null) {
            try {
                Files.walk(tempDir)
                        .sorted((a, b) -> -a.compareTo(b))
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException ignored) {
                            }
                        });
            } catch (IOException ignored) {
            }
        }
    }
}
