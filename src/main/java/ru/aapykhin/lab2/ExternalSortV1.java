package ru.aapykhin.lab2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExternalSortV1 implements ExternalSorter {

    private Path tempDir;

    @Override
    public String getName() {
        return "ExternalSortV1 (Basic)";
    }

    @Override
    public void sort(Path inputFile, Path outputFile, long maxMemoryBytes) throws IOException {
        tempDir = Files.createTempDirectory("external_sort_v1_");

        try {
            List<Path> sortedChunks = splitAndSort(inputFile, maxMemoryBytes);
            mergeChunks(sortedChunks, outputFile);
        } finally {
            cleanup();
        }
    }

    private List<Path> splitAndSort(Path inputFile, long maxMemoryBytes) throws IOException {
        List<Path> chunks = new ArrayList<>();
        List<String> currentChunk = new ArrayList<>();
        long currentChunkSize = 0;
        int chunkIndex = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                long lineSize = estimateLineMemory(line);

                if (currentChunkSize + lineSize > maxMemoryBytes && !currentChunk.isEmpty()) {
                    chunks.add(sortAndWriteChunk(currentChunk, chunkIndex++));
                    currentChunk.clear();
                    currentChunkSize = 0;
                }

                currentChunk.add(line);
                currentChunkSize += lineSize;
            }

            if (!currentChunk.isEmpty()) {
                chunks.add(sortAndWriteChunk(currentChunk, chunkIndex));
            }
        }

        return chunks;
    }

    private Path sortAndWriteChunk(List<String> chunk, int index) throws IOException {
        Collections.sort(chunk);

        Path chunkFile = tempDir.resolve("chunk_" + index + ".tmp");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFile.toFile()))) {
            for (String line : chunk) {
                writer.write(line);
                writer.newLine();
            }
        }

        return chunkFile;
    }

    private void mergeChunks(List<Path> chunks, Path outputFile) throws IOException {
        if (chunks.isEmpty()) {
            Files.createFile(outputFile);
            return;
        }

        if (chunks.size() == 1) {
            Files.move(chunks.get(0), outputFile);
            return;
        }

        List<BufferedReader> readers = new ArrayList<>();
        String[] currentLines = new String[chunks.size()];

        try {
            for (Path chunk : chunks) {
                BufferedReader reader = new BufferedReader(new FileReader(chunk.toFile()));
                readers.add(reader);
            }

            for (int i = 0; i < readers.size(); i++) {
                currentLines[i] = readers.get(i).readLine();
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toFile()))) {
                while (true) {
                    String minLine = null;
                    int minIndex = -1;

                    for (int i = 0; i < currentLines.length; i++) {
                        if (currentLines[i] != null) {
                            if (minLine == null || currentLines[i].compareTo(minLine) < 0) {
                                minLine = currentLines[i];
                                minIndex = i;
                            }
                        }
                    }

                    if (minIndex == -1) {
                        break;
                    }

                    writer.write(minLine);
                    writer.newLine();
                    currentLines[minIndex] = readers.get(minIndex).readLine();
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

    private long estimateLineMemory(String line) {
        return 40 + (long) line.length() * 2;
    }

    private void cleanup() {
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
