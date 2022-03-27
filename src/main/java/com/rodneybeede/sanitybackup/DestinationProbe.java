package com.rodneybeede.sanitybackup;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

/**
 * @author rbeede
 *
 *         Offers back complete in-memory collection of all files seen
 *         so they may be used for reporting later (like extras in dir
 *         report)
 */
public class DestinationProbe implements Callable<Set<Path>> {
	private static final Logger log = Logger.getLogger(DestinationProbe.class);

	private final Path startPath;

	public DestinationProbe(final Path startDir) {
		this.startPath = startDir;
	}

	@Override
	public Set<Path> call() throws Exception {
		final Set<Path> files = new TreeSet<>();
		
		Files.walkFileTree(this.startPath, new FileVisitor<Path>() {
			@Override
			public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
				files.add(file);
				
				if(log.isTraceEnabled()) {
					log.trace(file);
					log.trace(file.toAbsolutePath());
					log.trace(file.toUri());
					log.trace(file.toRealPath());
					log.trace(attrs.size());
					log.trace(attrs.lastModifiedTime());
				}
				
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
				log.error("Failed to access:  " + file, exc);
				
				return FileVisitResult.CONTINUE;
			}
		});
		
		
		return files;
	}

}
