package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.SchedulerProfile.HiddenOption;

import java.util.List;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_OWNER;
import static edu.utexas.tacc.tapis.systems.model.TSystem.EMPTY_STR_ARRAY;

/*
 * Class representing all scheduler profile attributes that can be set in an incoming create request json body
 */
public final class ReqPostSchedulerProfile
{
  public String name;
  public String description;
  public String owner = DEFAULT_OWNER;
  public String moduleLoadCommand;
  public String[] modulesToLoad = EMPTY_STR_ARRAY;
  public List<HiddenOption> hiddenOptions;
}
