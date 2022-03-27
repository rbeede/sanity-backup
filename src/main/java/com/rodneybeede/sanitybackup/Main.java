package com.rodneybeede.sanitybackup;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.AsyncAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Main {
	private static final Logger log = Logger.getLogger(Main.class);
	
	protected static final BlockingQueue<Path> filesToCopy = new LinkedBlockingQueue<>();
	protected static final List<String> corruptFiles = Collections.synchronizedList(new LinkedList<>());

	public static void main(final String[] args) throws IOException, InterruptedException {
		if(null == args || args.length != 2) {
			System.err.println("Incorrect number of arguments");
			System.out.println("Usage:  java -jar " + Main.class.getName() + " <source directory> <destination directory>");
			System.exit(255);
			return;
		}
		
		
		setupLogging();
				
		
		// Parse config options as Canonical paths
		// Real paths are needed since we must do src<->dest swapping later and don't want .. in anything
		final Path sourceDirectory = Paths.get(args[0]).toRealPath();
		final Path destinationDirectory = Paths.get(args[1]).toRealPath();
		
		log.info("Source directory (real canonical) is " + sourceDirectory);
		log.info("Destination directory (real canonical) is " + destinationDirectory);
		
		
		// General thread manager
		final ExecutorService executorService = Executors.newCachedThreadPool();
		
		
		// Start up the source listing data gather
		final Future<Set<Path>> sourceFuture = executorService.submit(new SourceProbe(sourceDirectory, destinationDirectory));
		
		
		// Create and start the copy workers
		final CopyWorker copyWorker1 = new CopyWorker(sourceDirectory, destinationDirectory);
		final CopyWorker copyWorker2 = new CopyWorker(sourceDirectory, destinationDirectory);
		copyWorker1.start();
		copyWorker2.start();
		
		
		// Start up the destination listing gatherer
		// Note that since new files are being copied this could be a listing that includes some newly copied files
		// already and not what was in destination solely before this program started
		final Future<Set<Path>> destinationFuture = executorService.submit(new DestinationProbe(destinationDirectory));
		
		
		// Time to wait on things to finish
		final Set<Path> sourceListing;
		try {
			sourceListing = sourceFuture.get();
		} catch (final ExecutionException e) {
			log.fatal("Unable to get complete source listing", e);
			System.exit(10);
			return;
		}
		
		log.info("Total number of source files/directories:  " + sourceListing.size());
		
		
		// At this point Main.filesToCopy has everything added that will be added
		
		
		// Now wait for all file copies to finish (sourceProbe has finished and added everything that needs copied
		while(!Main.filesToCopy.isEmpty()) {
			log.info("Waiting for all source files to finish copying");
			log.info(Main.filesToCopy.size() + " files remaining to be copied");
			
			Thread.sleep(10 * 1000);
		}
		
		
		// At this point Main.filesToCopy is empty, but the two worker threads could be busy with the last 1-2 items
		// If worker is waiting for worker signals to stop, otherwise allows last item to continue
		copyWorker1.signalShutdown();
		copyWorker2.signalShutdown();

		// Wait until workers are actually done
		log.info("Waiting for all " + CopyWorker.class.getSimpleName() + " to complete");
		while(copyWorker1.isAlive()) {
			log.trace("Waiting for copyWorker1 still...");
			Thread.sleep(10 * 1000);
		}
		while(copyWorker2.isAlive()) {
			log.trace("Waiting for copyWorker2 still...");
			Thread.sleep(10 * 1000);
		}
		
		
		// Wait on the destination listing results (if not already done)
		final Set<Path> destinationListing;
		try {
			destinationListing = destinationFuture.get();
		} catch (final ExecutionException e) {
			log.fatal("Unsuccessful in obtaining destination listing", e);
			System.exit(10);
			return;
		}
		
		log.debug("Found " + destinationListing.size() + " entries in destination listing");
		
		
		log.info("Generating file corruption report");
		// Pretty easy since SourceProbe already filled in report
		if(Main.corruptFiles.isEmpty()) {
			log.info("No corrupt files found");
		} else {
			log.error("CORRUPT FILES WERE FOUND!!!  MANUALLY VERIFY INTEGRITY OF SOURCE AND DEST!!!");
			log.error("" + System.lineSeparator()
						 + "Real Path\tSize in Bytes\tLast Modified FileTime" + System.lineSeparator()
						 + String.join(System.lineSeparator(), Main.corruptFiles)
						 + System.lineSeparator()
						);
		}
		
		log.info("Generating extras in destination report");
		final boolean foundExtras = reportExtras(sourceListing, destinationListing, sourceDirectory, destinationDirectory);
		
		
		// Exit with appropriate status
		log.info("Sync has completed");
		

		LogManager.shutdown();;  //Forces log to flush
		
		executorService.shutdown();
		
		if(!Main.corruptFiles.isEmpty()) {
			System.exit(101);
		} else if(foundExtras) {
			System.exit(2);  // Signal extras found
		} else {
			System.exit(0);  // All good
		}
	}
	
	
	private static void setupLogging() {
		final Layout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss,SSS Z}\t%-5p\tThread=%t\t%c\t%m%n");
		
		
		// Use an async logger for speed
		final AsyncAppender asyncAppender = new AsyncAppender();
		asyncAppender.setThreshold(Level.ALL);
		
		Logger.getRootLogger().setLevel(Level.ALL);
		Logger.getRootLogger().addAppender(asyncAppender);
		
		
		// Setup the logger to also log to the console
		final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
		consoleAppender.setEncoding("UTF-8");
		consoleAppender.setThreshold(Level.INFO);
		asyncAppender.addAppender(consoleAppender);
		
		
		// Setup the logger to log into the current working directory
		final File logFile = new File(System.getProperty("user.dir"), getFormattedDatestamp(null) + ".log");
		final FileAppender fileAppender;
		try {
			fileAppender = new FileAppender(layout, logFile.getAbsolutePath());
		} catch (final IOException e) {
			e.printStackTrace();
			log.error(e,e);
			return;
		}
		fileAppender.setEncoding("UTF-8");
		fileAppender.setThreshold(Level.ALL);
		asyncAppender.addAppender(fileAppender);
		
		System.out.println("Logging to " + logFile.getAbsolutePath());
	}
	
	
	private static String getFormattedDatestamp(final Date date) {
		final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_Z");
		
		if(null == date) {
			return dateFormat.format(new Date());
		} else {
			return dateFormat.format(date);
		}
	}
	
	
	private static boolean reportExtras(final Set<Path> sourceListing, final Set<Path> destinationListing, final Path sourceDirectory, final Path destinationDirectory) {
		boolean foundExtra = false;
		
		for(final Path currDest : destinationListing) {
			final Path expectedSource = FileUtilities.changeBase(currDest, destinationDirectory, sourceDirectory);
			
			if(!sourceListing.contains(expectedSource)) {
				try {
					log.warn("EXTRA FOUND IN DEST:  " + currDest + "\t" + Files.size(currDest) + " bytes\t" + Files.getLastModifiedTime(currDest));
				} catch (final IOException e) {
					log.error("EXTRA FOUND, REPORT ERROR:  Unable to stat dest file at:  " + currDest, e);
				}
				foundExtra = true;
			}
		}
		
		
		return foundExtra;
	}
}
