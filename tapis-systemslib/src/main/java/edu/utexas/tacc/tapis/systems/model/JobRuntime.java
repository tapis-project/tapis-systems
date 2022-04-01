package edu.utexas.tacc.tapis.systems.model;

/*
 * Class representing a runtime supported by the Tapis Jobs service.
 * For example DOCKER, SINGULARITY
 * Includes runtime type and version.
 */
public final class JobRuntime
{
  public enum RuntimeType {DOCKER, SINGULARITY}
  private final RuntimeType runtimeType;
  private final String version;

  public JobRuntime(RuntimeType runtimeType1, String version1)
  {
    runtimeType = runtimeType1;
    version = version1;
  }
  public RuntimeType getRuntimeType() { return runtimeType; }
  public String getVersion() { return version; }

  // Equals and hashcode auto-generated using com.google.auto.value.AutoValue and then copied here
  // Unable to use AutoValue directly due to gson deserialization
  @Override
  public boolean equals(Object o)
  {
    if (o == this) { return true; }
    if (o instanceof JobRuntime)
    {
      JobRuntime that = (JobRuntime) o;
      return this.runtimeType.equals(that.getRuntimeType()) && this.version.equals(that.getVersion());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= runtimeType.hashCode();
    h$ *= 1000003;
    h$ ^= version.hashCode();
    return h$;
  }
}
//import com.google.auto.value.AutoValue;
//import com.google.gson.Gson;
//import com.google.gson.TypeAdapter;
//import com.ryanharter.auto.value.gson.GenerateTypeAdapter;
/*
 * TODO/TBD: Override equals/hashCode so List.equals can work. For change history checking
 *           Make comparable?
 *           Use google's autoValue annotation for equals/hashCode?
 *           Better yet use java's new record keyword? But turns out this breaks gson deserialization.
 */
//public final class JobRuntime
//@GenerateTypeAdapter
//@AutoValue
//public abstract class JobRuntime
//{
//  public static JobRuntime create(RuntimeType runtimeType, String version) { return new AutoValue_JobRuntime(runtimeType, version);}
//  public static TypeAdapter<JobRuntime> typeAdapter(Gson gson) { return new AutoValue_JobRuntime.GsonTypeAdapter(gson);}
//  public enum RuntimeType {DOCKER, SINGULARITY}
//  public abstract RuntimeType getRuntimeType();
//  public abstract String getVersion();
//public record JobRuntime(RuntimeType type, String version)
//{
//  public enum RuntimeType {DOCKER, SINGULARITY}
//  public String getVersion() { return this.version; }
//}
