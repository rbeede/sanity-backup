package com.rodneybeede.sanitybackup;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.log4j.Logger;

public class FileUtilities {
	private static final Logger log = Logger.getLogger(FileUtilities.class);

	/**
	 * Works even when newBase has a different root from originalBase (e.g. C:\dir\file.txt to H:\dir\file.txt)
	 * 
	 * @param path
	 * @param originalBase
	 * @param newBase
	 * @return
	 * @throws IOException 
	 */
	public static Path changeBase(final Path path, final Path originalBase, final Path newBase) {
		log.trace("changeBase:  " + path + "\t" + originalBase + "\t" + newBase);
		
		// First convert path into relative from originalBase
		final Path relativeFile = originalBase.relativize(path);
		
		return newBase.resolve(relativeFile).normalize().toAbsolutePath();
	}
}
