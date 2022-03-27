package com.rodneybeede.sanitybackup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class CopyWorker extends Thread {
	private static final Logger log = Logger.getLogger(CopyWorker.class);
	
	private final Path sourceBase;
	private final Path destinationBase;
	
	private boolean shutdownSignalled = false;

	public CopyWorker(final Path sourceBase, final Path destinationBase) {
		this.sourceBase = sourceBase;
		this.destinationBase = destinationBase;
	}

	@Override
	public void run() {
		while(!shutdownSignalled) {  // Can't key off filesToCopy.isEmpty() because it can be empty at times but later get added to
			final Path sourceFile;
			
			try {
				sourceFile = Main.filesToCopy.poll(3, TimeUnit.SECONDS);
			} catch (final InterruptedException e) {
				if(this.shutdownSignalled) {
					log.debug("Received " + e.getClass().getName() + " signal to shutdown copy worker impatiently");
				} else {
					log.warn("Received " + e.getClass().getName() + " but no signal to shutdown copy worker cleanly");
				}
								
				return;
			}
			
			
			if(null == sourceFile)  continue;  // Do next check in loop for shutdown or wait for incoming data
			
			
			final Path destination = FileUtilities.changeBase(sourceFile, sourceBase, destinationBase);
			
			log.info("Copying " + sourceFile);
			
			
			// Just blindly do copy since it is in the queue
			// Some entries may actually be just directories and not files
			
			// First check to see if parent directories exist, if not create them since they must
			final Path destinationParentDir = destination.getParent();
			if(null == destinationParentDir) {
				// destination must be at root of file system which is okay
				log.trace(destination + " has no parent probably because it is root of file system");
			} else if(Files.notExists(destinationParentDir)) {
				// Need to create all the parent directories
				try {
					final BasicFileAttributes parentAttrs = Files.readAttributes(sourceFile.getParent(), BasicFileAttributes.class);
					Files.createDirectories(destinationParentDir, new FileAttribute<?>[0]);
					Files.setLastModifiedTime(destinationParentDir, parentAttrs.lastModifiedTime());
				} catch (final IOException e) {
					log.error("Failed during creation of all parent directories for destination file:  " + destination);
					log.debug("destination was:  " + destination);
					log.debug("source was:  " + sourceFile);
					log.debug("destinationParentDir:  " + destinationParentDir);
					continue;
				}
			} else if(!Files.isDirectory(destinationParentDir)) {
				// Problem here as file was created at parent instead of directory!
				log.error("Destination file/dir of " + destination + " has expected directory parent of " + destinationParentDir + " BUT parent was a file and not a directory!");
				log.error("Failed to copy source file:  " + sourceFile);
				continue;
			}  // else necessary parent directories already exist
			

			// Now copy the actual file (or directory)
			try {
				Files.copy(sourceFile, destination, StandardCopyOption.COPY_ATTRIBUTES);
			} catch (final Exception e) {
				log.error("Failed to copy " + sourceFile + " to " + destination, e);
				continue;
			}
			
			// Verify size and last modified date match as expected
			try {
				if(Files.size(sourceFile) != Files.size(destination) || !Files.getLastModifiedTime(sourceFile).equals(Files.getLastModifiedTime(destination))) {
					log.error("Corrupt file - MISMATCH AFTER COPY:  " + sourceFile + " to " + destination);
					Main.corruptFiles.add(sourceFile.toAbsolutePath() + "\t" + Files.size(sourceFile) + "\t" + Files.getLastModifiedTime(sourceFile));
					Main.corruptFiles.add(destination.toAbsolutePath() + "\t" + Files.size(destination) + "\t" + Files.getLastModifiedTime(destination));
				} else {
					log.info("Copied " + sourceFile + " to " + destination);
				}
			} catch (final Exception e) {
				log.fatal("Unable to stat/attr file system for either source or destination while on sourceFile of " + sourceFile, e);
				log.error("Unable to stat/attr file system for either source or destination while on sourceFile of " + sourceFile, e);
				continue;  // Keep trying on next file anyway
			}
		}
	}
	
	/**
	 * Sending a {@link Thread#interrupt()} is optional if you don't want to wait for the poll timeout
	 */
	public void signalShutdown() {
		this.shutdownSignalled = true;
	}

}
