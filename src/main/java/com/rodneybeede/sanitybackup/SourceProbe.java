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
public class SourceProbe implements Callable<Set<Path>> {
	private static final Logger log = Logger.getLogger(SourceProbe.class);

	private final Path startDir;
	private final Path destinationDir;

	public SourceProbe(final Path startDir, final Path destinationDir) {
		this.startDir = startDir;
		this.destinationDir = destinationDir;
	}

	@Override
	public Set<Path> call() throws Exception {
		final Set<Path> files = new TreeSet<>();
		
		log.trace("Beginning " + SourceProbe.class.getSimpleName() + " of " + this.startDir);
		
		Files.walkFileTree(this.startDir, new FileVisitor<Path>() {
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
				files.add(file);  // Remember for reporting later on of extra files
				
				final Path expectedDestFile = FileUtilities.changeBase(file, startDir, destinationDir);
				
				log.trace("Looking at expected dest of:  " + expectedDestFile);
				
				if(Files.notExists(expectedDestFile)) {
					log.trace(expectedDestFile + " does NOT exist and so needs copied");
					
					// Add to queue of files that need copied
					try {
						Main.filesToCopy.put(file);
					} catch (final InterruptedException e) {
						// Must be forced to shutdown
						log.fatal(e,e);
						return FileVisitResult.TERMINATE;
					}
				} else {
					log.trace(expectedDestFile + " appears to exist");
					
					// Verify existing dest file is not corrupt (has matching size and lastMod)
					if(attrs.size() != Files.size(expectedDestFile) || !attrs.lastModifiedTime().equals(Files.getLastModifiedTime(expectedDestFile))) {
						// Add to corrupt list
						Main.corruptFiles.add(file.toAbsolutePath() + "\t" + attrs.size() + "\t" + attrs.lastModifiedTime());
						Main.corruptFiles.add(expectedDestFile.toAbsolutePath() + "\t" + Files.size(expectedDestFile) + "\t" + Files.getLastModifiedTime(expectedDestFile));
						
						log.error("Corrupt file " + file.toAbsolutePath() + " to " + expectedDestFile.toAbsolutePath());
					} // otherwise no problem with existing file :)
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
