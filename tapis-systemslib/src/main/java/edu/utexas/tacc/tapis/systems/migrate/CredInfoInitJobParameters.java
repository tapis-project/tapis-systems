package edu.utexas.tacc.tapis.systems.migrate;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.apache.commons.lang3.Strings;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

/*
 * Parse, process and validate CredInfoInitJob parameters.
 *
 * Based on MigrateJobParameters
 */
public class CredInfoInitJobParameters
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(CredInfoInitJobParameters.class);

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // --------- Required Parameters -----------
  @Option(name = "-vtok", required = true, usage = "Token for Vault")
  public String vtok;

  @Option(name = "-vurl", required = true, usage = "Vault URL including port, ex: http(s)://host:32342")
  public String vurl;

  // --------- Optional Parameters -----------
  @Option(name = "--apply", usage = "Make permanent changes. By default it is a dry run.")
  public boolean isApply = false;

  @Option(name = "-help", aliases = {"--help", "-h", "-?"}, usage = "display help information")
  public boolean help;

  @Option(name = "-v", required = false, aliases = {"--verbose"}, usage = "write trace and debug log messages")
  public boolean verbose = false;

  @Option(name = "-q", required = false, aliases = {"--quiet"}, forbids = {"-v"}, usage = "suppress all log messages")
  public boolean quiet = false;

  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  public CredInfoInitJobParameters(String[] args) throws TapisException
  {
    initializeParms(args);
    validateParms();
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
        writer.write("CredInfoInitJob [options...]\n");
        ByteArrayOutputStream ostream = new ByteArrayOutputStream(initialCapacity);
        parser.printUsage(ostream);
        try {writer.write(ostream.toString(StandardCharsets.UTF_8));} catch (Exception e1) {/* ignore */}
        writer.write("\n");

        // Throw exception.
        throw new TapisException(writer.toString());
      }
    }

    // Display help and exit program.
    if (help)
    {
      String s = "\nCredInfoInitJob for Tapis Systems Service.";
      System.out.println(s);
      System.out.println("\nCredInfoInitJob [options...]\n");
      parser.printUsage(System.out);
      // Add a usage blurb.
      s = """
            
            CredInfoInitJob used to initialize the CredInfo table based on the records in Vault and SK.
            By default a dry run is made, no changes are applied.
            To apply changes use option --apply or set env variable TAPIS_MIGRATE_JOB_APPLY to "apply_changes\"""";
      System.out.println(s);
      System.exit(0);
    }
  }
  private void validateParms()
  {
    // Make sure there is no trailing slash in the url.
    vurl = Strings.CI.removeEnd(vurl, "/");
  }
}
