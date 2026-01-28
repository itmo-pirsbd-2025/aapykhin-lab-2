package ru.aapykhin.lab2.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class DataGenerator {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int MIN_LINE_LENGTH = 10;
    private static final int MAX_LINE_LENGTH = 1000;

    private final Random random;

    public DataGenerator() {
        this.random = new Random();
    }

    public DataGenerator(long seed) {
        this.random = new Random(seed);
    }

    public void generateFile(Path outputFile, long targetSizeBytes) throws IOException {
        Files.createDirectories(outputFile.getParent());

        long currentSize = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
            while (currentSize < targetSizeBytes) {
                String line = generateRandomLine();
                writer.write(line);
                writer.newLine();
                currentSize += line.length() + System.lineSeparator().length();
            }
        }
    }

    public String generateRandomLine() {
        int length = MIN_LINE_LENGTH + random.nextInt(MAX_LINE_LENGTH - MIN_LINE_LENGTH + 1);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }

    public void generateFileWithLineCount(Path outputFile, int lineCount) throws IOException {
        Files.createDirectories(outputFile.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
            for (int i = 0; i < lineCount; i++) {
                writer.write(generateRandomLine());
                writer.newLine();
            }
        }
    }
}
