package edu.utexas.tacc.tapis.systems.migrate;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/*
 * Parse, process and validate MigrateJob parameters.
 *
 * Based on SKAdminParameters from tapis-java repository: https://github.com/tapis-project/tapis-java
 */
public class MigrateJobParameters
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(MigrateJobParameters.class);

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // --------- Optional Parameters -----------
  @Option(name = "--apply", usage = "Make permanent changes. By default it is a dry run.")
  public boolean isApply = false;

  @Option(name = "-help", aliases = {"--help", "-h", "-?"}, usage = "display help information")
  public boolean help;


  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  public MigrateJobParameters(String[] args) throws TapisException
  {
    initializeParms(args);
  }

  /* **************************************************************************** */
  /*                               Public Methods                                 */
  /* **************************************************************************** */

  /* **************************************************************************** */
  /*                               Private Methods                                */
  /* **************************************************************************** */

  /**
   *  Parse the input arguments.
   */
  private void initializeParms(String[] args) throws TapisException
  {
    // Get a command line parser to verify input.
    CmdLineParser parser = new CmdLineParser(this);
    parser.getProperties().withUsageWidth(120);
    try
    {
      // Parse the arguments.
      parser.parseArgument(args);
    }
    catch (CmdLineException e)
    {
      if (!help)
      {
        // Create message buffer of sufficient size.
        final int initialCapacity = 1024;
        StringWriter writer = new StringWriter(initialCapacity);

        // Write parser error message.
        writer.write("\n******* Input Parameter Error *******\n");
        writer.write(e.getMessage());
        writer.write("\n\n");

        // Write usage information--unfortunately we need an output stream.
        writer.write("MigrateJob [options...]\n");
        ByteArrayOutputStream ostream = new ByteArrayOutputStream(initialCapacity);
        parser.printUsage(ostream);
        try {writer.write(ostream.toString(StandardCharsets.UTF_8));} catch (Exception e1) {}
        writer.write("\n");

        // Throw exception.
        throw new TapisException(writer.toString());
      }
    }

    // Display help and exit program.
    if (help)
    {
      String s = "\nMigrateJob for Tapis Systems Service.";
      System.out.println(s);
      System.out.println("\nMigrateJob [options...]\n");
      parser.printUsage(System.out);
      // Add a usage blurb.
      s = "\nMigrateJob used to perform a java based non-DB migration of Tapis Systems data.\n" +
          "By default a dry run is made, no changes are applied.\n" +
          "To apply changes use option --apply or set env variable TAPIS_MIGRATE_JOB_APPLY to \"apply_changes\"";
      System.out.println(s);
      System.exit(0);
    }
  }
}
