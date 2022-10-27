package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.SearchUtils;
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters.*;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;
import static org.testng.Assert.assertEquals;

/**
 * Test the SystemsDao getTSystems() call for various search use cases against a DB running locally
 * NOTE: This test pre-processes the search list just as is done in SystemsServiceImpl before it calls the Dao,
 *       including calling SearchUtils.validateAndProcessSearchCondition(cond)
 *       For this reason there is currently no need to have a SearchSystemsTest suite.
 *       If this changes then we will need to create another suite and move the test data into IntegrationUtils so that
 *       it can be re-used.
 */
@Test(groups={"integration"})
public class SearchDaoTest
{
  private SystemsDaoImpl dao;

  private ResourceRequestUser rOwner;

  // Test data
  private static final String testKey = "SrchGet";
  private static final String sysIdLikeAll = "id.like.*" + testKey + "*";

  // Strings for searches involving special characters
  private static final String specialChar7Str = ",()~*!\\"; // These 7 may need escaping
  private static final String specialChar7LikeSearchStr = "\\,\\(\\)\\~\\*\\!\\\\"; // All need escaping for LIKE/NLIKE
  private static final String specialChar7EqSearchStr = "\\,\\(\\)\\~*!\\"; // All but *! need escaping for other operators

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

  // String for search involving an escaped comma in a list of values
  private static final String escapedCommaInListValue = "abc\\,def";

  // Strings for char relational testings
  private static final String hostName1 = "host" + testKey + "001.test.org";
  private static final String hostName7 = "host" + testKey + "007.test.org";

  // Number of systems not including DTN systems
  int numSystems = 20;

  TSystem dtnSystem1 = IntegrationUtils.makeDtnSystem1(testKey);
  TSystem dtnSystem2 = IntegrationUtils.makeDtnSystem2(testKey);
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SearchDaoTest.class.getSimpleName());
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    ResourceRequestUser rUser = new ResourceRequestUser(new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(),
            null, apiUser, tenantName, null, null, null));

    // Initialize authenticated user
    rOwner = new ResourceRequestUser(new AuthenticatedUser(owner1, tenantName, TapisThreadContext.AccountType.user.name(),
                                     null, owner1, tenantName, null, null, null));
    // Cleanup anything leftover from previous failed run
    teardown();

    // Vary port # for checking numeric relational searches
    for (int i = 0; i < numSystems; i++) { systems[i].setPort(i+1); }
    // For half the systems change the owner
    for (int i = 0; i < numSystems/2; i++) { systems[i].setOwner(owner2); }

    // For one system update description to have some special characters. 7 special chars in value: ,()~*!\
    //   and update jobWorkingDir for testing an escaped comma in a list value
    systems[numSystems-1].setDescription(specialChar7Str);
    systems[numSystems-1].setJobWorkingDir(escapedCommaInListValue);

    // Create all the systems in the dB using the in-memory objects, recording start and end times
    LocalDateTime createBegin = TapisUtils.getUTCTimeNow();
    Thread.sleep(500);
    for (TSystem sys : systems)
    {
      boolean itemCreated = dao.createSystem(rUser, sys, gson.toJson(sys), rawDataEmtpyJson);
      Assert.assertTrue(itemCreated, "Item not created, id: " + sys.getId());
    }
    Thread.sleep(500);

    // Create a single system for testing timestamp equality
//    boolean itemCreated = dao.createSystem(authenticatedUser, dtnSystem1,  gson.toJson(dtnSystem1), scrubbedJson);
//    Assert.assertTrue(itemCreated, "Item not created, id: " + dtnSystem1.getId());
//    TSystem tmpSystem = dao.getSystem(tenantName, dtnSystem1.getId(), false);
//    Assert.assertNotNull(tmpSystem, "System not created. System name: " + dtnSystem1.getId());
//    created1 = LocalDateTime.ofInstant(tmpSystem.getCreated(), ZoneOffset.UTC);
//    created1Str = TapisUtils.getSQLStringFromUTCTime(created1);
//    System.out.println("Created System with id=" + dtnSystem1.getId() + " and created=" + created1Str);

    LocalDateTime createEnd = TapisUtils.getUTCTimeNow();
    System.out.println("Total time taken for BeforeSuite setup method: " + Duration.between(createBegin, createEnd));
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown for " + SearchDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    for (TSystem sys : systems)
    {
      dao.hardDeleteSystem(tenantName, sys.getId());
    }
//    dao.hardDeleteSystem(tenantName, dtnSystem1.getId());

    Assert.assertFalse(dao.checkForSystem(tenantName, systems[0].getId(), true),
                       "System not deleted. System name: " + systems[0].getId());
  }

  /*
   * Check valid cases
   */
  @Test(groups={"integration"})
  public void testValidCases() throws Exception
  {
    TSystem sys0 = systems[0];
    String sys0Id = sys0.getId();
    String nameList = "noSuchName1,noSuchName2," + sys0Id + ",noSuchName3";
    String tagList1 = String.format("%s", tagVal1);
    String tagList2 = String.format("%s,%s", tagVal1, tagVal2);
    String tagList3 = String.format("%s", tagVal3Space);
    String tagList4 = String.format("%s", tagValNotThere);
    // Create all input and validation data for tests
    // NOTE: Some cases require sysNameLikeAll in the list of conditions since maven runs the tests in
    //       parallel and not all attribute names are unique across integration tests
    class CaseData
    {
      public final int count;
      public final List<String> searchList;
      CaseData(int c, List<String> r) { count = c; searchList = r; }
    }
    var validCaseInputs = new HashMap<Integer, CaseData>();
    // Test basic types and operators
    validCaseInputs.put(1, new CaseData(1, List.of("id.eq." + sys0Id))); // 1 has specific name
    validCaseInputs.put(2, new CaseData(1, List.of("description.eq." + sys0.getDescription())));
    validCaseInputs.put(3, new CaseData(1, List.of("host.eq." + sys0.getHost())));
    validCaseInputs.put(4, new CaseData(1, List.of("bucket_name.eq." + sys0.getBucketName())));
    validCaseInputs.put(5, new CaseData(1, List.of("root_dir.eq." + sys0.getRootDir())));
    validCaseInputs.put(6, new CaseData(1, List.of("job_working_dir.eq." + sys0.getJobWorkingDir())));
    validCaseInputs.put(7, new CaseData(20, Arrays.asList(sysIdLikeAll, "batch_scheduler.eq." + sys0.getBatchScheduler())));
    validCaseInputs.put(8, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "batch_default_logical_queue.eq." + sys0.getBatchDefaultLogicalQueue())));
    validCaseInputs.put(9, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "batch_scheduler_profile.eq." + sys0.getBatchSchedulerProfile())));
    validCaseInputs.put(10, new CaseData(numSystems / 2, Arrays.asList(sysIdLikeAll, "owner.eq." + owner1)));  // Half owned by one user
    validCaseInputs.put(11, new CaseData(numSystems / 2, Arrays.asList(sysIdLikeAll, "owner.eq." + owner2))); // and half owned by another
    validCaseInputs.put(12, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "enabled.eq.true")));  // All are enabled
    validCaseInputs.put(13, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "deleted.eq.false"))); // none are deleted
    validCaseInputs.put(14, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "deleted.neq.true"))); // none are deleted
    validCaseInputs.put(15, new CaseData(0, Arrays.asList(sysIdLikeAll, "deleted.eq.true")));           // none are deleted
    validCaseInputs.put(16, new CaseData(1, List.of("id.like." + sys0Id)));
    validCaseInputs.put(17, new CaseData(0, List.of("id.like.NOSUCHSYSTEMxFM2c29bc8RpKWeE2sht7aZrJzQf3s")));
    validCaseInputs.put(18, new CaseData(numSystems, List.of(sysIdLikeAll)));
    validCaseInputs.put(19, new CaseData(numSystems - 1, Arrays.asList(sysIdLikeAll, "id.nlike." + sys0Id)));
    validCaseInputs.put(20, new CaseData(1, Arrays.asList(sysIdLikeAll, "id.in." + nameList)));
    validCaseInputs.put(21, new CaseData(numSystems - 1, Arrays.asList(sysIdLikeAll, "id.nin." + nameList)));
    validCaseInputs.put(22, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "system_type.eq.LINUX")));
    validCaseInputs.put(23, new CaseData(numSystems / 2, Arrays.asList(sysIdLikeAll, "system_type.eq.LINUX", "owner.neq." + owner2)));
    // Test Tapis3 specific CONTAINS operator used only for tags column
    validCaseInputs.put(24, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "tags.contains."+tagList1)));
    validCaseInputs.put(25, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "tags.contains."+tagList2)));
    validCaseInputs.put(26, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "tags.contains."+tagList3)));
    validCaseInputs.put(27, new CaseData(0, Arrays.asList(sysIdLikeAll, "tags.contains."+tagList4)));
    // Test numeric relational
    validCaseInputs.put(40, new CaseData(numSystems / 2, Arrays.asList(sysIdLikeAll, "port.between.1," + numSystems / 2)));
    validCaseInputs.put(41, new CaseData(numSystems / 2 - 1, Arrays.asList(sysIdLikeAll, "port.between.2," + numSystems / 2)));
    validCaseInputs.put(42, new CaseData(numSystems / 2, Arrays.asList(sysIdLikeAll, "port.nbetween.1," + numSystems / 2)));
    validCaseInputs.put(43, new CaseData(13, Arrays.asList(sysIdLikeAll, "enabled.eq.true", "port.lte.13")));
    validCaseInputs.put(44, new CaseData(5, Arrays.asList(sysIdLikeAll, "enabled.eq.true", "port.gt.1", "port.lt.7")));
    // Test char relational
    validCaseInputs.put(50, new CaseData(1, Arrays.asList(sysIdLikeAll, "host.lte." + hostName1)));
    validCaseInputs.put(51, new CaseData(numSystems - 7, Arrays.asList(sysIdLikeAll, "enabled.eq.true", "host.gt." + hostName7)));
    validCaseInputs.put(52, new CaseData(5, Arrays.asList(sysIdLikeAll, "host.gt." + hostName1, "host.lt." + hostName7)));
    validCaseInputs.put(53, new CaseData(0, Arrays.asList(sysIdLikeAll, "host.lte." + hostName1, "host.gt." + hostName7)));
    validCaseInputs.put(54, new CaseData(7, Arrays.asList(sysIdLikeAll, "host.between." + hostName1 + "," + hostName7)));
    validCaseInputs.put(55, new CaseData(numSystems - 7, Arrays.asList(sysIdLikeAll, "host.nbetween." + hostName1 + "," + hostName7)));
    // Test timestamp equality and relational
//    validCaseInputs.put(59, new CaseData(1, Arrays.asList(sysNameLikeAll, "created.eq." + created1Str)));
    validCaseInputs.put(60, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.gt." + longPast1)));
    validCaseInputs.put(61, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture1)));
    validCaseInputs.put(62, new CaseData(0, Arrays.asList(sysIdLikeAll, "created.lte." + longPast1)));
    validCaseInputs.put(63, new CaseData(0, Arrays.asList(sysIdLikeAll, "created.gte." + farFuture1)));
    validCaseInputs.put(64, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.between." + longPast1 + "," + farFuture1)));
    validCaseInputs.put(65, new CaseData(0, Arrays.asList(sysIdLikeAll, "created.nbetween." + longPast1 + "," + farFuture1)));
    // Variations of timestamp format
    validCaseInputs.put(66, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture2)));
    validCaseInputs.put(67, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture3)));
    validCaseInputs.put(68, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture4)));
    validCaseInputs.put(69, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture5)));
    validCaseInputs.put(70, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture6)));
    validCaseInputs.put(71, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture7)));
    validCaseInputs.put(72, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture8)));
    validCaseInputs.put(73, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture9)));
    validCaseInputs.put(74, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture10)));
    validCaseInputs.put(75, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture11)));
    validCaseInputs.put(76, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture12)));
    validCaseInputs.put(77, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture13)));
    validCaseInputs.put(78, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture14)));
    validCaseInputs.put(79, new CaseData(numSystems, Arrays.asList(sysIdLikeAll, "created.lt." + farFuture15)));
    // Test wildcards
    validCaseInputs.put(80, new CaseData(numSystems, Arrays.asList("enabled.eq.true", "host.like.host" + testKey + "*")));
    validCaseInputs.put(81, new CaseData(0, Arrays.asList(sysIdLikeAll, "enabled.eq.true", "host.nlike.host" + testKey + "*")));
    validCaseInputs.put(82, new CaseData(9, Arrays.asList(sysIdLikeAll, "enabled.eq.true", "host.like.host" + testKey + "00!.test.org")));
    validCaseInputs.put(83, new CaseData(11, Arrays.asList(sysIdLikeAll, "enabled.eq.true", "host.nlike.host" + testKey + "00!.test.org")));
    // Test that underscore and % get escaped as needed before being used as SQL
    validCaseInputs.put(90, new CaseData(0, Arrays.asList(sysIdLikeAll, "host.like.host" + testKey + "__")));
    validCaseInputs.put(91, new CaseData(0, Arrays.asList(sysIdLikeAll, "host.like.host" + testKey + "00%")));
    // Check various special characters in description. 7 special chars in value: ,()~*!\
    validCaseInputs.put(101, new CaseData(1, Arrays.asList(sysIdLikeAll, "description.like." + specialChar7LikeSearchStr)));
    validCaseInputs.put(102, new CaseData(numSystems - 1, Arrays.asList(sysIdLikeAll, "description.nlike." + specialChar7LikeSearchStr)));
    validCaseInputs.put(103, new CaseData(1, Arrays.asList(sysIdLikeAll, "description.eq." + specialChar7EqSearchStr)));
    validCaseInputs.put(104, new CaseData(numSystems - 1, Arrays.asList(sysIdLikeAll, "description.neq." + specialChar7EqSearchStr)));
    // Escaped comma in a list of values
    validCaseInputs.put(110, new CaseData(1, Arrays.asList(sysIdLikeAll, "job_working_dir.in." + "noSuchDir," + escapedCommaInListValue)));

    // Iterate over valid cases
    for (Map.Entry<Integer, CaseData> item : validCaseInputs.entrySet())
    {
      CaseData cd = item.getValue();
      int caseNum = item.getKey();
      System.out.println("Checking case # " + caseNum + " Input:        " + cd.searchList);
      // Build verified list of search conditions
      var verifiedSearchList = new ArrayList<String>();
      for (String cond : cd.searchList)
      {
        // Use SearchUtils to validate condition
        String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(cond);
        verifiedSearchList.add(verifiedCondStr);
      }
      System.out.println("  For case    # " + caseNum + " VerfiedInput: " + verifiedSearchList);
      List<TSystem> searchResults = dao.getSystems(rOwner, verifiedSearchList, null, DEFAULT_LIMIT,
                                                   orderByListNull,DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
      System.out.println("  Result size: " + searchResults.size());
      assertEquals(searchResults.size(), cd.count, "SearchDaoTest.testValidCases: Incorrect result count for case number: " + caseNum);
    }
  }

  /*
   * Test pagination options: limit, skip
   */
  @Test(groups={"integration"})
  public void testLimitSkip() throws Exception
  {
    String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(sysIdLikeAll);
    var verifiedSearchList = Collections.singletonList(verifiedCondStr);
    System.out.println("VerfiedInput: " + verifiedSearchList);
    List<TSystem> searchResults;

    int limit = -1;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    limit = 0;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 1;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 5;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 19;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 20;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    limit = 200;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    // Test limit + skip combination that reduces result size
    int resultSize = 3;
    limit = numSystems;
    int skip = limit - resultSize;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), resultSize, "Incorrect result count");

    // Check some corner cases
    limit = 100;
    skip = 0;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    limit = 0;
    skip = 1;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), 0, "Incorrect result count");
    limit = 10;
    skip = 15;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems - skip, "Incorrect result count");
    limit = 10;
    skip = 100;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListNull, skip, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), 0, "Incorrect result count");
  }

  /*
   * Test sorting: limit, orderBy, skip
   */
  @Test(groups={"integration"})
  public void testSortingSkip() throws Exception
  {
    String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(sysIdLikeAll);
    var verifiedSearchList = Collections.singletonList(verifiedCondStr);
    System.out.println("VerfiedInput: " + verifiedSearchList);
    List<TSystem> searchResults;

    int limit;
    int skip;
    // Sort and check order with no limit or skip
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, DEFAULT_LIMIT, orderByListAsc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    checkOrder(searchResults, 1, numSystems);
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, DEFAULT_LIMIT, orderByListDesc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    checkOrder(searchResults, numSystems, 1);
    // Sort and check order with limit and no skip
    limit = 4;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListAsc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    checkOrder(searchResults, 1, limit);
    limit = 19;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListDesc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    checkOrder(searchResults, numSystems, numSystems - (limit-1));
    // Sort and check order with limit and skip
    limit = 2;
    skip = 5;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListAsc, skip, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_006 to SrchGet_007
    checkOrder(searchResults, skip + 1, skip + limit);
    limit = 4;
    skip = 3;
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListDesc, skip, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_017 to SrchGet_014
    checkOrder(searchResults, numSystems - skip, numSystems - limit);

    // Sort and check multiple orderBy
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, DEFAULT_LIMIT, orderByList2Asc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    checkOrder(searchResults, 1, numSystems);
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, DEFAULT_LIMIT, orderByList2Desc, DEFAULT_SKIP, startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count");
    checkOrder(searchResults, numSystems, 1);
  }

  /*
   * Test sorting: limit, orderBy, startAfter
   */
  @Test(groups={"integration"})
  public void testSortingStartAfter() throws Exception
  {
    String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(sysIdLikeAll);
    var verifiedSearchList = Collections.singletonList(verifiedCondStr);
    System.out.println("VerfiedInput: " + verifiedSearchList);
    List<TSystem> searchResults;

    int limit;
    int startAfterIdx;
    String startAfter;
    // Sort and check order with limit and startAfter
    limit = 2;
    startAfterIdx = 5;
    startAfter = getSysName(testKey, startAfterIdx);
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListAsc, DEFAULT_SKIP, startAfter, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_006 to SrchGet_007
    checkOrder(searchResults, startAfterIdx + 1, startAfterIdx + limit);
    limit = 4;
    startAfterIdx = 18;
    int startWith = numSystems - startAfterIdx + 1;
    startAfter = getSysName(testKey, startAfterIdx);
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByListDesc, DEFAULT_SKIP, startAfter, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_017 to SrchGet_014
    checkOrder(searchResults, numSystems - startWith, numSystems - limit);

    // Sort and check multiple orderBy (second order orderBy column has no effect but at least we can check that
    //    having it does not break things for startAfter
    limit = 2;
    startAfterIdx = 5;
    startAfter = getSysName(testKey, startAfterIdx);
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByList3Asc, DEFAULT_SKIP, startAfter, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    assertEquals(searchResults.size(), limit, "Incorrect result count");
    // Should get systems named SrchGet_006 to SrchGet_007
    checkOrder(searchResults, startAfterIdx + 1, startAfterIdx + limit);
    limit = 4;
    startAfterIdx = 18;
    startWith = numSystems - startAfterIdx + 1;
    startAfter = getBucketName(testKey, startAfterIdx);
    searchResults = dao.getSystems(rOwner, verifiedSearchList, null, limit,  orderByList3Desc, DEFAULT_SKIP, startAfter, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
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
