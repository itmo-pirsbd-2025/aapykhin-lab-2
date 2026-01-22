package ru.aapykhin.lab2.benchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import ru.aapykhin.lab2.ExternalSortV1;
import ru.aapykhin.lab2.ExternalSortV2;
import ru.aapykhin.lab2.ExternalSorter;
import ru.aapykhin.lab2.util.DataGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 10, time = 5)
@Fork(1)
public class ExternalSortBenchmark {

    @Param({"10", "50"})
    private int fileSizeMb;

    @Param({"1", "4", "16"})
    private int chunkSizeMb;

    private Path inputFile;
    private Path outputFileV1;
    private Path outputFileV2;
    private Path tempDir;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("benchmark_");
        inputFile = tempDir.resolve("input_" + fileSizeMb + "mb.txt");
        outputFileV1 = tempDir.resolve("output_v1.txt");
        outputFileV2 = tempDir.resolve("output_v2.txt");

        System.out.println("Generating " + fileSizeMb + " MB test file...");
        DataGenerator generator = new DataGenerator(42);
        generator.generateFile(inputFile, (long) fileSizeMb * 1024 * 1024);
        System.out.println("Test file generated: " + Files.size(inputFile) / 1024 / 1024 + " MB");
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                    }
                });
    }

    @TearDown(Level.Invocation)
    public void cleanupOutput() throws IOException {
        Files.deleteIfExists(outputFileV1);
        Files.deleteIfExists(outputFileV2);
    }

    @Benchmark
    public void sortV1(Blackhole bh) throws IOException {
        ExternalSorter sorter = new ExternalSortV1();
        sorter.sort(inputFile, outputFileV1, (long) chunkSizeMb * 1024 * 1024);
        bh.consume(Files.size(outputFileV1));
    }

    @Benchmark
    public void sortV2(Blackhole bh) throws IOException {
        ExternalSorter sorter = new ExternalSortV2();
        sorter.sort(inputFile, outputFileV2, (long) chunkSizeMb * 1024 * 1024);
        bh.consume(Files.size(outputFileV2));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ExternalSortBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
