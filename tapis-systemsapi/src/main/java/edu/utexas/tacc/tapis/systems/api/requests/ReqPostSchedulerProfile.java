package edu.utexas.tacc.tapis.systems.api.requests;

import java.util.List;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_OWNER;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile.HiddenOption;
import edu.utexas.tacc.tapis.systems.model.ModuleLoadSpec;

/*
 * Class representing all scheduler profile attributes that can be set in an incoming create request json body
 */
public final class ReqPostSchedulerProfile
{
  public String name;
  public String description;
  public String owner = DEFAULT_OWNER;
  public List<ModuleLoadSpec> moduleLoads;
  public List<HiddenOption> hiddenOptions;
}
