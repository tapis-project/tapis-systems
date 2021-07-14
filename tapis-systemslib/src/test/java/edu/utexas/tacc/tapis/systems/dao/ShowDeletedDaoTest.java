package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.model.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters.DEFAULT_LIMIT;
import static edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters.DEFAULT_SKIP;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.apiUser;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.gson;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.orderByListNull;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.scrubbedJson;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.searchASTNull;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.setOfIDsNull;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.showDeletedFalse;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.showDeletedTrue;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.startAfterNull;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.tenantName;
import static org.testng.Assert.assertEquals;

/**
 * Test the SystemsDao getTSystems() call to verify that deleted systems are filtered properly.
 */
@Test(groups={"integration"})
public class ShowDeletedDaoTest
{
  private SystemsDaoImpl dao;
  private ResourceRequestUser rUser;

  // Test data
  private static final String testKey = "SrchDel";
  private static final String sysIdLikeAll = "id.like.*" + testKey + "*";

  // Number of systems not including DTN systems
  int numSystems = 3;

  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + ShowDeletedDaoTest.class.getSimpleName());
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    rUser = new ResourceRequestUser(new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(),
                                                          null, apiUser, tenantName, null, null, null));

    // Cleanup anything leftover from previous failed run
    teardown();

    for (TSystem sys : systems)
    {
      boolean itemCreated = dao.createSystem(rUser, sys, gson.toJson(sys), scrubbedJson);
      Assert.assertTrue(itemCreated, "Item not created, id: " + sys.getId());
    }
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown for " + ShowDeletedDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    for (TSystem sys : systems)
    {
      dao.hardDeleteSystem(tenantName, sys.getId());
    }
    Assert.assertFalse(dao.checkForSystem(tenantName, systems[0].getId(), true),
                       "System not deleted. System name: " + systems[0].getId());
  }

  /*
   * All cases
   * Confirm search/get with none deleted.
   * Delete one system.
   * Confirm Get and Search return one less system.
   * Undelete the system.
   * Confirm get/search counts are back to total.
   */
  @Test(groups={"integration"})
  public void testSearchGetDeleted() throws Exception
  {
    TSystem sys0 = systems[0];
    String sys0Id = sys0.getId();
    var searchListAll= Collections.singletonList(SearchUtils.validateAndProcessSearchCondition(sysIdLikeAll));

    // None deleted yet so should have all systems
    // First check counts. showDeleted = true or false should have total number of systems.
    int count = dao.getSystemsCount(tenantName, searchListAll, searchASTNull, setOfIDsNull, orderByListNull,
                                    startAfterNull, showDeletedFalse);
    assertEquals(count, numSystems, "Incorrect count for getSystemsCount/showDel=false before delete of system");
    count = dao.getSystemsCount(tenantName, searchListAll, searchASTNull, setOfIDsNull, orderByListNull,
                                startAfterNull, showDeletedTrue);
    assertEquals(count, numSystems, "Incorrect count for getSystemsCount/showDel=true before delete of system");
    // Check retrieving all systems
    List<TSystem> searchResults = dao.getSystems(tenantName, searchListAll, searchASTNull, setOfIDsNull, DEFAULT_LIMIT,
                                                 orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count for getSystems/showDel=false");
    searchResults = dao.getSystems(tenantName, searchListAll, searchASTNull, setOfIDsNull, DEFAULT_LIMIT,
                                   orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedTrue);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count for getSystems/showDel=true");

    // Now delete a system
    dao.updateDeleted(rUser, tenantName, sys0Id, true);

    // First check counts. showDeleted = false should return 1 less than total.
    count = dao.getSystemsCount(tenantName, searchListAll, searchASTNull, setOfIDsNull, orderByListNull,
                                startAfterNull, showDeletedFalse);
    assertEquals(count, numSystems-1, "Incorrect count for getSystemsCount/showDel=false after delete of system");
    count = dao.getSystemsCount(tenantName, searchListAll, searchASTNull, setOfIDsNull, orderByListNull,
                                startAfterNull, showDeletedTrue);
    assertEquals(count, numSystems, "Incorrect count for getSystemsCount/showDel=true after delete of system");

    // Check retrieving all systems
    searchResults = dao.getSystems(tenantName, searchListAll, searchASTNull, setOfIDsNull, DEFAULT_LIMIT,
                                   orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedFalse);
    assertEquals(searchResults.size(), numSystems-1, "Incorrect result count for getSystems/showDel=false after delete of system");
    searchResults = dao.getSystems(tenantName, searchListAll, searchASTNull, setOfIDsNull, DEFAULT_LIMIT,
                                   orderByListNull, DEFAULT_SKIP, startAfterNull, showDeletedTrue);
    assertEquals(searchResults.size(), numSystems, "Incorrect result count for getSystems/showDel=true after delete of system");
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

}
