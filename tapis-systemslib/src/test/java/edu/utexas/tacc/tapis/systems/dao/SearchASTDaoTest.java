package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTParser;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

import static edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters.*;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;

/**
 * Test the SystemsDao getTSystems call when using an ASTNode for various search use cases against a DB running locally
 * NOTE: This test pre-processes the sql-like search string just as is done in SystemsServiceImpl before it calls the Dao.
 *       For this reason there is currently no need to have a SearchSystemsTest suite.
 *       If this changes then we will need to create another suite and move the test data into IntegrationUtils so that
 *       it can be re-used.
 */
@Test(groups={"integration"})
public class SearchASTDaoTest
{
  private SystemsDaoImpl dao;
  private ResourceRequestUser rUser, rOwner;

  // Test data
  private static final String testKey = "SrchAST";
  private static final String sysNameLikeAll = "id LIKE '" + "%" + testKey + "%'";

  // Strings for searches involving special characters
  private static final String specialChar3Str = ".:_"; // Values containing these must be surrounded by single quotes.
  private static final String specialChar7Str = ",()~*!\\"; // These 7 may need escaping
  private static final String specialChar7LikeSearchStr = ",()~*!\\\\"; // Only \ needs escaping for LIKE/NLIKE

  // String for search involving an escaped comma in a list of values
  private static final String escapedCommanInListValue = "abc\\,def";

  // Timestamps in various formats
  private static final String longPast1 =   "1800-01-01T00:00:00.123456Z";
  private static final String farFuture1 =  "2200-04-29T14:15:52.123456-06:00";
  private static final String farFuture2 =  "2200-04-29T14:15:52.123Z";
  private static final String farFuture3 =  "2200-04-29T14:15:52.123";
  private static final String farFuture4 =  "2200-04-29T14:15:52-06:00";
  private static final String farFuture5 =  "2200-04-29T14:15:52";
  private static final String farFuture6 =  "2200-04-29T14:15-06:00";
  private static final String farFuture7 =  "2200-04-29T14:15";
  private static final String farFuture8 =  "2200-04-29T14-06:00";
  private static final String farFuture9 =  "2200-04-29T14";
  private static final String farFuture10 = "2200-04-29-06:00";
  private static final String farFuture11 = "2200-04-29";
  private static final String farFuture12 = "2200-04Z";
  private static final String farFuture13 = "2200-04";
  private static final String farFuture14 = "2200Z";
  private static final String farFuture15 = "2200";

  int numSystems = 20;
  TSystem dtnSystem1 = IntegrationUtils.makeDtnSystem1(testKey);
  TSystem dtnSystem2 = IntegrationUtils.makeDtnSystem2(testKey);
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);

  LocalDateTime createBegin;
  LocalDateTime createEnd;

  // Set of IDs for collecting systems owned by owner2
  Set<String> owner2IDSet = new HashSet<>();

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SearchASTDaoTest.class.getSimpleName());
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    rUser = new ResourceRequestUser(new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(),
                                                          null, apiUser, tenantName, null, null, null));
    rOwner = new ResourceRequestUser(new AuthenticatedUser(owner1, tenantName, TapisThreadContext.AccountType.user.name(),
                                                           null, owner1, tenantName, null, null, null));

    // Cleanup anything leftover from previous failed run
    teardown();

    // Vary port # for checking numeric relational searches
    for (int i = 0; i < numSystems; i++) { systems[i].setPort(i+1); }
    // For half the systems change the owner
    for (int i = 0; i < numSystems/2; i++) { systems[i].setOwner(owner2); owner2IDSet.add(systems[i].getId());}

    // For one system update description to have some special characters. 7 special chars in value: ,()~*!\
    //   and update workingDir for testing an escaped comma in a list value
    systems[numSystems-1].setDescription(specialChar7Str);
    systems[numSystems-1].setJobWorkingDir(escapedCommanInListValue);

    systems[numSystems-2].setDescription(specialChar3Str);

    // Create all the systems in the dB using the in-memory objects, recording start and end times
    createBegin = TapisUtils.getUTCTimeNow();
    Thread.sleep(500);
    for (TSystem sys : systems)
    {
      boolean itemCreated = dao.createSystem(rOwner, sys, gson.toJson(sys), rawDataEmptyJson);
      Assert.assertTrue(itemCreated, "Item not created, id: " + sys.getId());
    }
    Thread.sleep(500);
    createEnd = TapisUtils.getUTCTimeNow();
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown for " + SearchASTDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    for (TSystem sys : systems)
    {
      dao.hardDeleteSystem(tenantName, sys.getId());
    }
    Assert.assertFalse(dao.checkForSystem(tenantName, systems[0].getId(), true),
                      "System not deleted. System name: " + systems[0].getId());
  }

  /*
   * Check valid cases
   */
  @Test(groups={"integration"})
  public void testValidCases() throws Exception
  {
    String[] hostNames = new String[numSystems];
    for (int i = 0; i < numSystems; i++) hostNames[i] = systems[i].getHost();
    TSystem sys0 = systems[0];
    String sys0Name = sys0.getId();
    String nameList = String.format("('noSuchName1','noSuchName2','%s','noSuchName3')", sys0Name);
    String tagList1 = String.format("('%s')", tagVal1);
    String tagList2 = String.format("('%s','%s')", tagVal1, tagVal2);
    String tagList3 = String.format("('%s')", tagVal3Space);
    String tagList4 = String.format("('%s')", tagValNotThere);
    // Create all input and validation data for tests
    // NOTE: Some cases require sysNameLikeAll in the list of conditions since maven runs the tests in
    //       parallel and not all attribute names are unique across integration tests
    class CaseData {public final int count; public final String sqlSearchStr; CaseData(int c, String s) { count = c; sqlSearchStr = s; }}
    var validCaseInputs = new HashMap<Integer, CaseData>();
    // Test basic types and operators
    validCaseInputs.put( 1,new CaseData(1, String.format("id = '%s'",sys0Name))); // 1 has specific name
    validCaseInputs.put( 2,new CaseData(1, String.format("description = '%s'",sys0.getDescription())));
    validCaseInputs.put( 3,new CaseData(1, String.format("host = '%s'",sys0.getHost())));
    validCaseInputs.put( 4,new CaseData(1, String.format("bucket_name = '%s'",sys0.getBucketName())));
    validCaseInputs.put( 5,new CaseData(1, String.format("root_dir = '%s'",sys0.getRootDir())));
    validCaseInputs.put( 6,new CaseData(1, String.format("job_working_dir = '%s'",sys0.getJobWorkingDir())));
    validCaseInputs.put( 7,new CaseData(20, sysNameLikeAll + String.format(" AND batch_scheduler = '%s'",sys0.getBatchScheduler().name())));
    validCaseInputs.put( 8,new CaseData(numSystems, sysNameLikeAll + String.format("AND batch_default_logical_queue = '%s'",sys0.getBatchDefaultLogicalQueue())));
    validCaseInputs.put( 9,new CaseData(numSystems, sysNameLikeAll + String.format("AND batch_scheduler_profile = '%s'",sys0.getBatchSchedulerProfile())));
    validCaseInputs.put(10,new CaseData(numSystems/2, sysNameLikeAll + String.format(" AND owner = '%s'",owner1)));  // Half owned by one user
    validCaseInputs.put(11,new CaseData(numSystems/2, sysNameLikeAll + String.format(" AND owner = '%s'",owner2))); // and half owned by another
    validCaseInputs.put(12,new CaseData(numSystems, sysNameLikeAll + " AND enabled = true"));  // All are enabled
    validCaseInputs.put(13,new CaseData(numSystems, sysNameLikeAll + " AND deleted = false")); // none are deleted
    validCaseInputs.put(14,new CaseData(numSystems, sysNameLikeAll + " AND deleted <> true")); // none are deleted
    validCaseInputs.put(15,new CaseData(0, sysNameLikeAll + " AND deleted = true"));           // none are deleted
    validCaseInputs.put(16,new CaseData(1, String.format("id LIKE '%s'",sys0Name)));
    validCaseInputs.put(17,new CaseData(0, "id LIKE 'NOSUCHSYSTEMxFM2c29bc8RpKWeE2sht7aZrJzQf3s'"));
    validCaseInputs.put(18,new CaseData(numSystems, sysNameLikeAll));
    validCaseInputs.put(19,new CaseData(numSystems-1, sysNameLikeAll + String.format(" AND id NLIKE '%s'",sys0Name)));
    validCaseInputs.put(20,new CaseData(1, sysNameLikeAll + " AND id IN " + nameList));
    validCaseInputs.put(21,new CaseData(numSystems-1, sysNameLikeAll + " AND id NIN " + nameList));
    validCaseInputs.put(22,new CaseData(numSystems, sysNameLikeAll + " AND system_type = LINUX"));
    validCaseInputs.put(23,new CaseData(numSystems/2, sysNameLikeAll +  String.format(" AND system_type = LINUX AND owner <> '%s'",owner2)));
    // Test Tapis3 specific CONTAINS operator used only for tags column
    validCaseInputs.put(24, new CaseData(numSystems, String.format("%s AND tags IN %s", sysNameLikeAll, tagList1)));
    validCaseInputs.put(25, new CaseData(numSystems, String.format("%s AND tags IN %s", sysNameLikeAll, tagList2)));
    validCaseInputs.put(26, new CaseData(numSystems, String.format("%s AND tags IN %s", sysNameLikeAll, tagList3)));
    validCaseInputs.put(27, new CaseData(0, String.format("%s AND tags NIN %s", sysNameLikeAll, tagList1)));
    validCaseInputs.put(28, new CaseData(0, String.format("%s AND tags IN %s", sysNameLikeAll, tagList4)));

    // Check various special characters require modified handling in AST. Special chars in value: _:.
    validCaseInputs.put(30,new CaseData(1, sysNameLikeAll + String.format(" AND description = '%s'",specialChar3Str)));
    validCaseInputs.put(31,new CaseData(1, sysNameLikeAll + String.format(" AND description LIKE '%s'",specialChar3Str)));

    // Test numeric relational
    validCaseInputs.put(50,new CaseData(numSystems/2, sysNameLikeAll + String.format(" AND port BETWEEN '%d' AND '%d'",1,numSystems/2)));
    validCaseInputs.put(51,new CaseData(numSystems/2-1, sysNameLikeAll + String.format(" AND port BETWEEN '%d' AND '%d'",2,numSystems/2)));
    validCaseInputs.put(52,new CaseData(numSystems/2, sysNameLikeAll + String.format(" AND port NBETWEEN '%d' AND '%d'",1,numSystems/2)));

    validCaseInputs.put(53,new CaseData(13, sysNameLikeAll + " AND enabled = true AND port <= '13'"));
    validCaseInputs.put(54,new CaseData(5, sysNameLikeAll + " AND enabled = true AND port > '1' AND port < '7'"));
    // Test char relational
    validCaseInputs.put(70,new CaseData(1, sysNameLikeAll + String.format(" AND host < '%s'",hostNames[1])));
    validCaseInputs.put(71,new CaseData(numSystems-8, sysNameLikeAll + String.format(" AND enabled = true AND host > '%s'",hostNames[7])));
    validCaseInputs.put(72,new CaseData(5, sysNameLikeAll + String.format(" AND host > '%s' AND host < '%s'",hostNames[1],hostNames[7])));
    validCaseInputs.put(73,new CaseData(0, sysNameLikeAll + String.format(" AND host < '%s' AND host > '%s'",hostNames[1],hostNames[7])));
    validCaseInputs.put(74,new CaseData(7, sysNameLikeAll + String.format(" AND host BETWEEN '%s' AND '%s'",hostNames[1],hostNames[7])));
    validCaseInputs.put(75,new CaseData(numSystems-7, sysNameLikeAll + String.format(" AND host NBETWEEN '%s' AND '%s'",hostNames[1],hostNames[7])));

    // Test timestamp relational
    validCaseInputs.put(90,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created > '%s'",longPast1)));
    validCaseInputs.put(91,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture1)));
    validCaseInputs.put(92,new CaseData(0, sysNameLikeAll + String.format(" AND created <= '%s'",longPast1)));
    validCaseInputs.put(93,new CaseData(0, sysNameLikeAll + String.format(" AND created >= '%s'",farFuture1)));
    validCaseInputs.put(94,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created BETWEEN '%s' AND '%s'",longPast1,farFuture1)));
    validCaseInputs.put(95,new CaseData(0, sysNameLikeAll + String.format(" AND created NBETWEEN '%s' AND '%s'",longPast1,farFuture1)));
    // Variations of timestamp format
    validCaseInputs.put(96,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture2)));
    validCaseInputs.put(97,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture3)));
    validCaseInputs.put(98,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture4)));
    validCaseInputs.put(99,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture5)));
    validCaseInputs.put(100,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture6)));
    validCaseInputs.put(101,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture7)));
    validCaseInputs.put(102,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture8)));
    validCaseInputs.put(103,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture9)));
    validCaseInputs.put(104,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture10)));
    validCaseInputs.put(105,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture11)));
    validCaseInputs.put(106,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture12)));
    validCaseInputs.put(107,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture13)));
    validCaseInputs.put(108,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture14)));
    validCaseInputs.put(109,new CaseData(numSystems, sysNameLikeAll + String.format(" AND created < '%s'",farFuture15)));

    // Test wildcards
    validCaseInputs.put(130,new CaseData(numSystems, "enabled = true AND host LIKE 'host" + testKey + "%'"));
    validCaseInputs.put(131,new CaseData(0, sysNameLikeAll + " AND enabled = true AND host NLIKE 'host" + testKey + "%'"));
    validCaseInputs.put(132,new CaseData(9, sysNameLikeAll + " AND enabled = true AND host LIKE 'host" + testKey + "00_.test.org'"));
    validCaseInputs.put(133,new CaseData(11, sysNameLikeAll + " AND enabled = true AND host NLIKE 'host" + testKey + "00_.test.org'"));

    // Check various special characters in description. 7 special chars in value: ,()~*!\
    validCaseInputs.put(171,new CaseData(1, sysNameLikeAll + String.format(" AND description LIKE '%s'",specialChar7LikeSearchStr)));
    validCaseInputs.put(172,new CaseData(numSystems-1, sysNameLikeAll + String.format(" AND description NLIKE '%s'",specialChar7LikeSearchStr)));
    validCaseInputs.put(173,new CaseData(1, sysNameLikeAll + String.format(" AND description = '%s'",specialChar7Str)));
    validCaseInputs.put(174,new CaseData(numSystems-1, sysNameLikeAll + String.format(" AND description <> '%s'",specialChar7Str)));
    // Escaped comma in a list of values
    validCaseInputs.put(200,new CaseData(1, sysNameLikeAll + String.format(" AND job_working_dir IN ('noSuchDir','%s')",escapedCommanInListValue)));

    // Iterate over valid cases
    for (Map.Entry<Integer,CaseData> item : validCaseInputs.entrySet())
    {
      CaseData cd = item.getValue();
      int caseNum = item.getKey();
      System.out.println("Checking case # " + caseNum + " Input:        " + cd.sqlSearchStr);
      // Build an AST from the sql-like search string
      ASTNode searchAST = ASTParser.parse(cd.sqlSearchStr);
      System.out.println("  Created AST with leaf node count: " + searchAST.countLeaves());
      List<TSystem> searchResults = dao.getSystems(rOwner, null, null, searchAST, DEFAULT_LIMIT, orderByListNull,
                                                   DEFAULT_SKIP, startAfterNull, showDeletedFalse,
                                                   listTypeAll, owner2IDSet, setOfIDsNull);
      System.out.println("  Result size: " + searchResults.size());
      assertEquals(searchResults.size(), cd.count, "SearchASTDaoTest.testValidCases: Incorrect result count for case number: " + caseNum);
    }
  }

  /*
   * Test pagination options: limit, skip
   */
  @Test(groups={"integration"})
  public void testLimitSkip() throws Exception
  {
    String searchStr = sysNameLikeAll;
    ASTNode searchAST = ASTParser.parse(searchStr);
    System.out.println("SearchSQL: " + searchStr);
    List<TSystem> searchResults;

    int limit = -1;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, DEFAULT_SKIP, startAfterNull,
                                   showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    limit = 0;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, DEFAULT_SKIP, startAfterNull,
                                   showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 1;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 5;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 19;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 20;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 200;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    // Test limit + skip combination that reduces result size
    int resultSize = 3;
    limit = numSystems;
    int skip = limit - resultSize;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), resultSize, "Incorrect result count");

    // Check some corner cases
    limit = 100;
    skip = 0;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    limit = 0;
    skip = 1;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), 0, "Incorrect result count");
    limit = 10;
    skip = 15;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems - skip, "Incorrect result count");
    limit = 10;
    skip = 100;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), 0, "Incorrect result count");
  }

  /*
   * Test sorting: limit, orderBy, skip
   */
  @Test(groups={"integration"})
  public void testSortingSkip() throws Exception
  {
    String searchStr = sysNameLikeAll;
    ASTNode searchAST = ASTParser.parse(searchStr);
    System.out.println("SearchSQL: " + searchStr);
    List<TSystem> searchResults;

    int limit;
    int skip;
    // Sort and check order with no limit or skip
    searchResults = dao.getSystems(rOwner, null, null, searchAST, DEFAULT_LIMIT, orderByListAsc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    checkOrder(searchResults, 1, numSystems);
    searchResults = dao.getSystems(rOwner, null, null, searchAST, DEFAULT_LIMIT, orderByListDesc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    checkOrder(searchResults, numSystems, 1);
    // Sort and check order with limit and no skip
    limit = 4;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListAsc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    checkOrder(searchResults, 1, limit);
    limit = 19;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListDesc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    checkOrder(searchResults, numSystems, numSystems - (limit-1));
    // Sort and check order with limit and skip
    limit = 2;
    skip = 5;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListAsc, skip, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_006 to SrchGet_007
    checkOrder(searchResults, skip + 1, skip + limit);
    limit = 4;
    skip = 3;
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListDesc, skip, startAfterNull, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_017 to SrchGet_014
    checkOrder(searchResults, numSystems - skip, numSystems - limit);
  }

  /*
   * Test sorting: limit, orderBy, startAfter
   */
  @Test(groups={"integration"})
  public void testSortingStartAfter() throws Exception
  {
    String searchStr = sysNameLikeAll;
    ASTNode searchAST = ASTParser.parse(searchStr);
    System.out.println("SearchSQL: " + searchStr);
    List<TSystem> searchResults;

    int limit;
    int startAfterIdx;
    String startAfter;
    // Sort and check order with limit and startAfter
    limit = 2;
    startAfterIdx = 5;
    startAfter = getSysName(testKey, startAfterIdx);
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListAsc, DEFAULT_SKIP, startAfter, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_006 to SrchGet_007
    checkOrder(searchResults, startAfterIdx + 1, startAfterIdx + limit);
    limit = 4;
    startAfterIdx = 18;
    int startWith = numSystems - startAfterIdx + 1;
    startAfter = getSysName(testKey, startAfterIdx);
    searchResults = dao.getSystems(rOwner, null, null, searchAST, limit, orderByListDesc, DEFAULT_SKIP, startAfter, showDeletedFalse, listTypeAll, owner2IDSet, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_017 to SrchGet_014
    checkOrder(searchResults, numSystems - startWith, numSystems - limit);
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

  /**
   * Check that results were sorted in correct order when sorting on system name
   */
  private void checkOrder(List<TSystem> searchResults, int start, int end)
  {
    int idx = 0; // Position in result
    // Name should match for loop counter i
    if (start < end)
    {
      for (int i = start; i <= end; i++)
      {
        String sysName = getSysName(testKey, i);
        assertEquals(searchResults.get(idx).getId(), sysName, "Incorrect system name at position: " + (idx+1));
        idx++;
      }
    }
    else
    {
      for (int i = start; i >= end; i--)
      {
        String sysName = getSysName(testKey, i);
        assertEquals(searchResults.get(idx).getId(), sysName, "Incorrect system name at position: " + (idx+1));
        idx++;
      }
    }
  }
}
