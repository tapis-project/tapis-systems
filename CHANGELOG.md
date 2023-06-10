
# Change Log for Tapis Systems Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/systems.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

---------------------------------------------------------------------------
## 1.3.4 - 2023-06-10

Incremental improvements and new features.

### Breaking changes:
- None

### New features:
- Add notes and inputMode attributes to items in jobEnvVariables.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.3.3 - 2023-06-01

Incremental improvements and bug fixes

### Breaking changes:
- None

### New features:
- Add application shutdown hook for graceful shutdown.

### Bug fixes:
- When Files fetches a system the effectiveUserId is incorrect for the mapped login user case. 
- Do not log error if optional startup parameter for applying a migration is not set.

---------------------------------------------------------------------------
## 1.3.2 - 2023-04-25

Incremental improvements.

### New features:
- Child Systems

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.3.1 - 2023-04-02

Incremental improvements and bug fix.

### New features:
- None

### Bug fixes:
- Update sharedAppCtx to represent app share grantor.

---------------------------------------------------------------------------
## 1.3.0 - 2023-02-24

Incremental improvements and new preview features.

### Breaking changes:
- Support for multiple module load entries in a scheduler profile has changed the data structure. The modules entry is now an array modules to load.
  - https://tapis-project.github.io/live-docs/?service=Systems#tag/Scheduler-Profiles

### New features:
- Added enableCmdPrefix to Systems. This can be used to enable/disable command prefix for jobs.
- Add accessToken, refreshToken to Credential for Globus support.
- Add support for Globus. New credential attributes *accessToken*, *refreshToken*, new AuthnMethod type TOKEN. New endpoints:
    - GET  /v3/systems/credential/{systemId}/globus/authUrl
    - POST /v3/systems/credential/{systemId}/globus/{authCode}/tokens/{userName}
- Support multiple module load entries for SchedulerProfile.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.2.8 - 2023-01-10

Incremental improvements and new preview features.

### New features:
- Add attributes returned for getSystem: *isPublic*, *isDynamicEffectiveUser*.
- Remove support for *resolveEffectiveUser* in favor of new attribute *isDynamicEffectiveUser*.
- Add endpoint for checking credentials.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.2.7 - 2022-11-14

Incremental improvements and new preview features.

### New features:
- Add support for searching by *tags* attribute using operator *contains*.
- Add support for query parameter *listType* when retrieving systems. Allows for filtering based on authorization.
    * Options are OWNED, SHARED_PUBLIC, ALL. Default is OWNED.
- Improved error message when attempting to search using an unsupported attribute
- Use ForbiddenException (403) as appropriate.

---------------------------------------------------------------------------
## 1.2.6 - 2022-10-25

Incremental improvements and new preview feature.

### New features:
- Add *description* attribute to *jobEnvVariables*.

---------------------------------------------------------------------------
## 1.2.5 - 2022-09-27

Incremental improvements and bug fixes.

### Bug fixes:
- Fix issues with handling of authorization checks for service requests.
- Fix issues with sharing support.

---------------------------------------------------------------------------
## 1.2.4 - 2022-09-19

1. Patched system authorization code to allow any service to read any system definition.
2. Fixed system authorization code for publicly shared system to allow user to add their own credentials.

## 1.2.4 - 2022-08-07

Incremental improvements and new preview features.

### New features:
- Support sharedAppCtx for getSystem. Allowed for Jobs and Files services.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.2.3 - 2022-07-22

Incremental improvements and new preview feature.

### New features:
- Support query parameter resolveEffectiveUser for geSystem, getSystems

### Bug fixes:
- Switch default for *resolveEffectiverUser* from false to true.

---------------------------------------------------------------------------
## 1.2.2 - 2022-07-20

Incremental improvements and new preview feature.

### New features:
- Support query parameter resolveEffectiveUser for geSystem, getSystems

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.2.1 - 2022-06-10

Incremental improvements and bug fixes

### New features:
- None

### Bug fixes:
- Fix bugs in login user mapping support.

---------------------------------------------------------------------------
## 1.2.0 - 2022-05-23

Incremental improvements and new features.

### New features:
- Support mapping of a Tapis user to a login user when effectiveUserId is dynamic.
- Store secrets in SK under separate paths for static and dynamic effectiveUserId.
- Standalone java program (MigrateJob.java) for performing a one-time migration of data for the Systems service.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.1.4 - 2022-05-07

Incremental improvements and preview of new features.

### New features:
- Refactor authorization checks for maintainability.
- Replace skipTapisAuthorization with impersonationId for requests from Jobs, Files and Apps services.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.1.3 - 2022-04-08

Preview of new features.

### New features:
- Additional information for System history.
- Support skipTapisAuthorization for requests from Jobs, Files and Apps services.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.1.2 - 2022-03-18

System history implementation.

### New features:
- System history end point implementation.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.1.1 - 2022-03-03

Incremental improvements and bug fixes.

### New features:
- Update readyCheck to check for expired service JWT.
- Updates for JDK 17

### Bug fixes:
- Fix issue with credential check at create/update time. Check was not being done when effectiveUserId set to ${apiUserId} 

---------------------------------------------------------------------------
## 1.1.0 - 2022-01-07

New minor release.

### New features:
- Add minLength to mpiCmd in jsonschema.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.0.3 - 2021-12-15

Incremental improvements and bug fixes.

### New features:
- Add mpiCmd attribute.

### Bug fixes:
- Fix bug in setting default for SchedulerProfile owner.

---------------------------------------------------------------------------
## 1.0.2 - 2021-11-12

Incremental improvements and bug fixes.

### New features:
- Rename jobIsBatch to canRunBatch.
- Support for public builds from repository.
- Set scheduler profile owner to apiUserId if owner not specified.
- Source code cleanup and refactoring.
- Add check for existence of scheduler profile

### Bug fixes:
- Allow null for imporRefId and Capability.value
- Fix several bugs related to scheduler profiles.
- Fix jobRuntimes PUT bug.

---------------------------------------------------------------------------
## 1.0.1 - 2021-10-20

Incremental improvements.

### New features:
 - Add importRefId attribute.
 - Support for multiple orderBy in search and sort.
 - Add support for scheduler profiles.
 - Add credential check during system and credential updates. On by default. Use skipCredentialCheck=true to bypass.
 - Source code cleanup and refactoring.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.0.0 - 2021-07-16

Initial release supporting basic CRUD operations on Tapis System resources
as well as management of Tapis System user credentials and permissions.

### Breaking Changes:
- Initial release.

### New features:
- Initial release.

### Bug fixes:
- None
