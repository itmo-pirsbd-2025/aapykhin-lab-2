package ru.aapykhin.lab2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.aapykhin.lab2.util.DataGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExternalSortTest {

    private Path tempDir;
    private Path inputFile;
    private Path outputFileV1;
    private Path outputFileV2;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("external_sort_test_");
        inputFile = tempDir.resolve("input.txt");
        outputFileV1 = tempDir.resolve("output_v1.txt");
        outputFileV2 = tempDir.resolve("output_v2.txt");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                    }
                });
    }

    @Test
    void testV1SortsCorrectly() throws IOException {
        List<String> lines = generateTestLines(1000);
        Files.write(inputFile, lines);

        ExternalSorter sorter = new ExternalSortV1();
        sorter.sort(inputFile, outputFileV1, 10 * 1024);

        List<String> sortedLines = Files.readAllLines(outputFileV1);
        List<String> expected = new ArrayList<>(lines);
        Collections.sort(expected);

        assertEquals(expected.size(), sortedLines.size());
        assertEquals(expected, sortedLines);
    }

    @Test
    void testV2SortsCorrectly() throws IOException {
        List<String> lines = generateTestLines(1000);
        Files.write(inputFile, lines);

        ExternalSorter sorter = new ExternalSortV2();
        sorter.sort(inputFile, outputFileV2, 10 * 1024);

        List<String> sortedLines = Files.readAllLines(outputFileV2);
        List<String> expected = new ArrayList<>(lines);
        Collections.sort(expected);

        assertEquals(expected.size(), sortedLines.size());
        assertEquals(expected, sortedLines);
    }

    @Test
    void testBothVersionsProduceSameResult() throws IOException {
        List<String> lines = generateTestLines(500);
        Files.write(inputFile, lines);

        new ExternalSortV1().sort(inputFile, outputFileV1, 5 * 1024);
        new ExternalSortV2().sort(inputFile, outputFileV2, 5 * 1024);

        List<String> resultV1 = Files.readAllLines(outputFileV1);
        List<String> resultV2 = Files.readAllLines(outputFileV2);

        assertEquals(resultV1, resultV2, "V1 and V2 should produce identical results");
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 500, 1000, 5000})
    void testV1WithDifferentSizes(int lineCount) throws IOException {
        List<String> lines = generateTestLines(lineCount);
        Files.write(inputFile, lines);

        ExternalSorter sorter = new ExternalSortV1();
        sorter.sort(inputFile, outputFileV1, 8 * 1024);

        List<String> sortedLines = Files.readAllLines(outputFileV1);
        List<String> expected = new ArrayList<>(lines);
        Collections.sort(expected);

        assertEquals(expected, sortedLines);
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 500, 1000, 5000})
    void testV2WithDifferentSizes(int lineCount) throws IOException {
        List<String> lines = generateTestLines(lineCount);
        Files.write(inputFile, lines);

        ExternalSorter sorter = new ExternalSortV2();
        sorter.sort(inputFile, outputFileV2, 8 * 1024);

        List<String> sortedLines = Files.readAllLines(outputFileV2);
        List<String> expected = new ArrayList<>(lines);
        Collections.sort(expected);

        assertEquals(expected, sortedLines);
    }

    @Test
    void testEmptyFile() throws IOException {
        Files.createFile(inputFile);

        new ExternalSortV1().sort(inputFile, outputFileV1, 1024);
        new ExternalSortV2().sort(inputFile, outputFileV2, 1024);

        assertTrue(Files.exists(outputFileV1));
        assertTrue(Files.exists(outputFileV2));
        assertEquals(0, Files.size(outputFileV1));
        assertEquals(0, Files.size(outputFileV2));
    }

    @Test
    void testSingleLine() throws IOException {
        Files.write(inputFile, List.of("single line"));

        new ExternalSortV1().sort(inputFile, outputFileV1, 1024);
        new ExternalSortV2().sort(inputFile, outputFileV2, 1024);

        assertEquals(List.of("single line"), Files.readAllLines(outputFileV1));
        assertEquals(List.of("single line"), Files.readAllLines(outputFileV2));
    }

    @Test
    void testAlreadySorted() throws IOException {
        List<String> lines = List.of("aaa", "bbb", "ccc", "ddd", "eee");
        Files.write(inputFile, lines);

        new ExternalSortV1().sort(inputFile, outputFileV1, 50);
        new ExternalSortV2().sort(inputFile, outputFileV2, 50);

        assertEquals(lines, Files.readAllLines(outputFileV1));
        assertEquals(lines, Files.readAllLines(outputFileV2));
    }

    @Test
    void testReverseSorted() throws IOException {
        List<String> lines = List.of("eee", "ddd", "ccc", "bbb", "aaa");
        Files.write(inputFile, lines);

        new ExternalSortV1().sort(inputFile, outputFileV1, 50);
        new ExternalSortV2().sort(inputFile, outputFileV2, 50);

        List<String> expected = List.of("aaa", "bbb", "ccc", "ddd", "eee");
        assertEquals(expected, Files.readAllLines(outputFileV1));
        assertEquals(expected, Files.readAllLines(outputFileV2));
    }

    @Test
    void testWithDataGenerator() throws IOException {
        DataGenerator generator = new DataGenerator(42);
        generator.generateFileWithLineCount(inputFile, 1000);

        List<String> originalLines = Files.readAllLines(inputFile);
        List<String> expected = new ArrayList<>(originalLines);
        Collections.sort(expected);

        new ExternalSortV1().sort(inputFile, outputFileV1, 20 * 1024);
        assertEquals(expected, Files.readAllLines(outputFileV1));

        new ExternalSortV2().sort(inputFile, outputFileV2, 20 * 1024);
        assertEquals(expected, Files.readAllLines(outputFileV2));
    }

    private List<String> generateTestLines(int count) {
        DataGenerator generator = new DataGenerator(12345);
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            lines.add(generator.generateRandomLine());
        }
        return lines;
    }
}
