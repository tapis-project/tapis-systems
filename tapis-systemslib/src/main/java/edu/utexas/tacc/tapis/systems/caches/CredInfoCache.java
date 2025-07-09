package edu.utexas.tacc.tapis.systems.caches;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;

import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

/*
 * CredentialInfo cache. Loads CredentialInfo records from the DB
 */
@Service
public class CredInfoCache
{
  private static final Logger log = LoggerFactory.getLogger(CredInfoCache.class);

  // Cache timeout
  private static final long CACHE_TIMEOUT_HOURS = 24;

  private final LoadingCache<CredInfoCacheKey, CredentialInfo> cache;
  private final ResourceRequestUser rUser;
  private final SystemsDao dao;

  @Inject
  public CredInfoCache(ResourceRequestUser rUser1, SystemsDao dao1)
  {
    rUser = rUser1;
    dao = dao1;
    cache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofHours(CACHE_TIMEOUT_HOURS)).build(new CredentialInfoLoader());
  }

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /*
   * Return cache size
   */
  public long getSize() { return cache.size(); }

  /*
   * Get credInfo record from the cache
   */
  public CredentialInfo getCredentialInfo(String tenantId, String systemId, String tapisUser, String hostLoginUser,
                                          boolean isStatic)
  {
    try
    {
      CredInfoCacheKey key = new CredInfoCacheKey(tenantId, systemId, tapisUser, hostLoginUser, isStatic);
      return cache.get(key);
    }
    catch (ExecutionException ex)
    {
      String msg = LibUtils.getMsg("SYSLIB_CREDINFO_CACHE_FETCH_ERR", tenantId, systemId, tapisUser, hostLoginUser, isStatic, ex.getMessage());
      throw new WebApplicationException(msg, ex);
    }
  }

  /*
   * Invalidate a cache entry
   */
  public void invalidateEntry(@NotNull String tenant, @NotNull String sysId, @NotNull String tapisUser,
                              @NotNull String hostLoginUser, boolean isStatic)
  {
    CredInfoCacheKey key = new CredInfoCacheKey(tenant, sysId, tapisUser, hostLoginUser, isStatic);
    cache.invalidate(key);
  }

  // ====================================================================================
  // =======  Private Classes ===========================================================
  // ====================================================================================

  /**
   * Class implementing method needed for populating the cache.
   */
  private class CredentialInfoLoader extends CacheLoader<CredInfoCacheKey, CredentialInfo>
  {
    @NotNull
    @Override
    public CredentialInfo load(CredInfoCacheKey key)
    {
      TSystem sys = dao.getSystem(key.tenantId, key.systemId);
      CredentialInfo credInfo = dao.getCredInfo(key.tenantId, key.systemId, key.tapisUser, key.isStatic);
      // If no record in DB then create in-memory record and DB record
      if (credInfo == null)
      {
        credInfo = new CredentialInfo(sys.getSeqId(), key.tenantId, key.systemId, key.tapisUser, key.isStatic,
                                      key.hostLoginUser, null, CredentialInfo.SyncStatus.PENDING);
        credInfo = dao.createCredInfo(rUser, credInfo);
      }
      return credInfo;
    }
  }

  /**
   * Class representing the cache key.
   * Unique keys for tenantId+systemId+tapisUser+isStatic
   */
  private static class CredInfoCacheKey
  {
    private final String tenantId;
    private final String systemId;
    private final String tapisUser;
    private final String hostLoginUser;
    private final boolean isStatic;

    public CredInfoCacheKey(String tenantId1, String systemId1, String tapisUser1, String hostLoginUser1, boolean isStatic1)
    {
      systemId = systemId1;
      tenantId = tenantId1;
      tapisUser = tapisUser1;
      hostLoginUser = hostLoginUser1;
      isStatic = isStatic1;
    }

    // ====================================================================================
    // =======  Accessors =================================================================
    // ====================================================================================
    public String getTenantId() { return tenantId; }
    public String getSystemId() { return systemId; }
    public String getTapisUser() { return tapisUser; }
    public String getHostLoginUser() { return hostLoginUser; }
    public boolean isStatic() { return isStatic; }

    // ====================================================================================
    // =======  Support for equals ========================================================
    // ====================================================================================
    @Override
    public boolean equals(Object o)
    {
      if (o == this) return true;
      // Note: no need to check for o==null since instanceof will handle that case
      if (!(o instanceof CredInfoCacheKey)) return false;
      var that = (CredInfoCacheKey) o;
      return (Objects.equals(this.tenantId, that.tenantId) && Objects.equals(this.systemId, that.systemId) &&
              Objects.equals(this.tapisUser, that.tapisUser) && Objects.equals(this.hostLoginUser, that.hostLoginUser) &&
              this.isStatic == that.isStatic);
    }

    @Override
    public int hashCode() { return Objects.hash(tenantId, systemId, tapisUser, hostLoginUser, isStatic); }
  }
}