package ru.aapykhin.lab2;

import java.io.IOException;
import java.nio.file.Path;

public interface ExternalSorter {
    void sort(Path inputFile, Path outputFile, long maxMemoryBytes) throws IOException;
    String getName();
}
