
# Change Log for Tapis Systems Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/systems.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

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
- None.

---------------------------------------------------------------------------
## 1.0.0 - 2021-07-16

Initial release supporting basic CRUD operations on Tapis System resources
as well as management of Tapis System user credentials and permissions.

### Breaking Changes:
- Initial release.

### New features:
- Initial release.

### Bug fixes:
- None.
